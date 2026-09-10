package status

import (
	"context"
	stderrors "errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	InProgressStatus = "in progress"
	SuccessStatus    = "success"
	CancelStatus     = "cancel"
	ErrorStatus      = "error"
)

var Current = &AsyncStatus{}

const NotFromAPI = int(-1)

// apiServerMode is set once when the API server starts. The server re-enters the
// same cli.App in process for POST /backup/actions, and every such re-entry comes
// from a handler which already owns its status row — or deliberately has none,
// when the command is listed in api.backup_actions_skip_commands. Either way the
// CLI wrapper must not register anything of its own in this process.
var apiServerMode atomic.Bool

// SetAPIServerMode marks this process as an API server. Never reset.
func SetAPIServerMode() {
	apiServerMode.Store(true)
}

// APIServerMode reports whether this process runs the API server.
func APIServerMode() bool {
	return apiServerMode.Load()
}

type AsyncStatus struct {
	// commands is the ordered history. Rows are addressed by a monotonic id
	// rather than by position, because trimLocked drops finished rows from
	// anywhere in the history while ids handed out earlier must stay valid.
	commands []*ActionRow
	byId     map[int]*ActionRow
	nextId   int
	sync.RWMutex
}

// DefaultMaxFinishedRows is used until SetMaxFinishedRows is called from a loaded
// config, so a status list created before any config is read is still bounded.
const DefaultMaxFinishedRows = 1000

// maxFinishedRows bounds how many finished rows are kept in memory. Long running
// `watch` processes register one row per iteration, so without a bound the history
// grows for as long as the process lives, see
// https://github.com/Altinity/clickhouse-backup/issues/1481
var maxFinishedRows = DefaultMaxFinishedRows

// SetMaxFinishedRows applies general.status_history_size. Safe to call from the
// API server on config reload, and from `watch` before its first iteration.
// Non-positive values are ignored, ValidateConfig already rejects them.
func SetMaxFinishedRows(n int) {
	if n > 0 {
		maxFinishedRows = n
	}
}

type ActionRowStatus struct {
	Command     string `json:"command"`
	Status      string `json:"status"`
	Start       string `json:"start,omitempty"`
	Finish      string `json:"finish,omitempty"`
	Error       string `json:"error,omitempty"`
	OperationId string `json:"operation_id,omitempty"`
	// Result carries a command specific JSON payload, currently the DryRunReport
	// of a `--dry-run` command, https://github.com/Altinity/clickhouse-backup/issues/1012
	Result string `json:"result,omitempty"`
}

type ActionRow struct {
	ActionRowStatus
	// id is the value handed to callers as commandId, stable for the row's life.
	id     int
	Ctx    context.Context
	Cancel context.CancelFunc
	// Done is closed by Stop when the command goroutine has fully returned.
	// Cancel/CancelAll wait on this so callers know the operation really
	// finished (e.g. defers like pidlock.RemovePidFile have already run).
	Done chan struct{}
	// startedAt is the monotonic counterpart of ActionRowStatus.Start, used to
	// report an exact duration in the completion callback.
	startedAt time.Time
	// callback is nil when the command must not produce a completion callback.
	callback *CallbackConfig
}

// CancelWaitTimeout bounds how long Cancel/CancelAll wait for the command
// goroutine to return. After the timeout we give up and let the caller
// proceed (a stuck goroutine should not block /backup/kill forever).
// The default matches APIConfig.CancelOperationTimeout (1800s); the server
// overrides it from config on Restart/ReloadConfig.
var CancelWaitTimeout = 1800 * time.Second

// SetCancelWaitTimeout updates the global CancelWaitTimeout. Safe to call
// from the API server when reloading config.
func SetCancelWaitTimeout(d time.Duration) {
	if d > 0 {
		CancelWaitTimeout = d
	}
}

func (status *AsyncStatus) Start(command string) (int, context.Context) {
	return status.StartWithOperationId(command, "")
}

func (status *AsyncStatus) StartWithOperationId(command string, operationId string) (int, context.Context) {
	return status.StartWithCallback(command, operationId, nil)
}

// StartWithCallback registers a command and attaches the completion callback
// configuration to it. Passing a nil callback, an empty URL list or a command
// which is not CallbackEligible means no callback is sent when it finishes.
func (status *AsyncStatus) StartWithCallback(command string, operationId string, callback *CallbackConfig) (int, context.Context) {
	status.Lock()
	defer status.Unlock()
	now := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	if callback != nil && (len(callback.URLs) == 0 || !CallbackEligible(command)) {
		callback = nil
	}
	if status.byId == nil {
		status.byId = map[int]*ActionRow{}
	}
	row := &ActionRow{
		ActionRowStatus: ActionRowStatus{
			Command:     command,
			Start:       now.Format(common.TimeFormat),
			Status:      InProgressStatus,
			OperationId: operationId,
		},
		id:        status.nextId,
		Ctx:       ctx,
		Cancel:    cancel,
		Done:      make(chan struct{}),
		startedAt: now,
		callback:  callback,
	}
	status.nextId++
	status.commands = append(status.commands, row)
	status.byId[row.id] = row
	log.Debug().Msgf("api.status.Start -> status.commands[%d] == %+v", row.id, *row)
	status.trimLocked()
	return row.id, ctx
}

// trimLocked drops the oldest finished rows once more than maxFinishedRows of
// them accumulate. Rows still in progress are always kept regardless of age, so
// a long living `watch` or `server` row does not block trimming of the finished
// iterations recorded after it. Lock MUST be held.
func (status *AsyncStatus) trimLocked() {
	finished := 0
	for _, row := range status.commands {
		if trimmableLocked(row) {
			finished++
		}
	}
	drop := finished - maxFinishedRows
	if drop <= 0 {
		return
	}
	kept := make([]*ActionRow, 0, len(status.commands)-drop)
	for _, row := range status.commands {
		if drop > 0 && trimmableLocked(row) {
			delete(status.byId, row.id)
			drop--
			continue
		}
		kept = append(kept, row)
	}
	status.commands = kept
}

// trimmableLocked reports whether a row can be forgotten. A terminal status is
// not enough: Cancel/CancelAll mark a row canceled while its command goroutine
// is still running, and that goroutine still has to reach Stop to close Done,
// release the Cancel waiter and send the callback it owes. Dropping the row
// before that would strand /backup/kill for CancelWaitTimeout and lose the
// notification. Lock MUST be held.
func trimmableLocked(row *ActionRow) bool {
	if row.Status == InProgressStatus {
		return false
	}
	if row.Done == nil {
		return true
	}
	select {
	case <-row.Done:
		return true
	default:
		return false
	}
}

// rowLocked resolves a commandId to a row, or nil when it was already trimmed
// or never existed. Lock MUST be held.
func (status *AsyncStatus) rowLocked(commandId int) *ActionRow {
	return status.byId[commandId]
}

func (status *AsyncStatus) CheckCommandInProgress(command string) bool {
	status.RLock()
	defer status.RUnlock()
	for _, cmd := range status.commands {
		if cmd.Command == command && cmd.Status == InProgressStatus {
			return true
		}
	}
	return false
}

// InProgress any .Status == InProgressStatus command shall return true, https://github.com/Altinity/clickhouse-backup/issues/827
func (status *AsyncStatus) InProgress() bool {
	status.RLock()
	defer status.RUnlock()
	for n := range status.commands {
		if status.commands[n].Status == InProgressStatus {
			log.Debug().Msgf("api.status.inProgress -> status.commands[%d].Status == %s, inProgress=%v", n, status.commands[n].Status, status.commands[n].Status == InProgressStatus)
			return true
		}
	}

	log.Debug().Msgf("api.status.inProgress -> len(status.commands)=%d, inProgress=false", len(status.commands))
	return false
}

func (status *AsyncStatus) GetContextWithCancel(commandId int) (context.Context, context.CancelFunc, error) {
	status.RLock()
	defer status.RUnlock()
	if commandId == NotFromAPI {
		ctx, cancel := context.WithCancel(context.Background())
		return ctx, cancel, nil
	}
	row := status.rowLocked(commandId)
	if row == nil {
		return nil, nil, errors.Errorf("commandId=%d not exists in current running commands", commandId)
	}
	if row.Ctx == nil {
		return nil, nil, errors.Errorf("commands[%d]=%s have nil context ", commandId, row.Command)
	}
	// for create_remote and restore_remote API call
	if stderrors.Is(row.Ctx.Err(), context.Canceled) && strings.Contains(row.Command, "_remote") {
		row.Ctx, row.Cancel = context.WithCancel(context.Background())
	}
	return row.Ctx, row.Cancel, nil
}

// SetResult attaches a command specific JSON payload to a row, so it becomes
// visible in /backup/status, GET /backup/actions and system.backup_actions.
// MUST be called before Stop, a poller which sees a finished row expects the
// result to be there already.
func (status *AsyncStatus) SetResult(commandId int, result string) {
	if result == "" {
		return
	}
	status.Lock()
	defer status.Unlock()
	row := status.rowLocked(commandId)
	if row == nil {
		log.Warn().Msgf("api.status.setResult -> commandId=%d not found", commandId)
		return
	}
	row.Result = result
}

func (status *AsyncStatus) Stop(commandId int, err error) {
	status.Lock()
	row := status.rowLocked(commandId)
	if row == nil {
		status.Unlock()
		log.Warn().Msgf("api.status.stop -> commandId=%d not found", commandId)
		return
	}
	// Always signal "goroutine finished" to any Cancel waiter, even if the
	// row was already moved to cancel/error/success state by a concurrent
	// Cancel() call.
	closeDoneLocked(row)
	if row.Status != InProgressStatus {
		// Already terminal, typically moved to CancelStatus by Cancel/CancelAll.
		// The callback is still owed to the caller, Cancel() itself does not send
		// it because the command goroutine has not returned yet at that point.
		callback, payload := status.finishLocked(row)
		status.Unlock()
		if callback != nil {
			go notify(callback, payload)
		}
		return
	}
	row.Cancel()
	s := SuccessStatus
	if err != nil {
		s = ErrorStatus
		row.Error = err.Error()
	}
	row.Status = s
	row.Finish = time.Now().Format(common.TimeFormat)
	row.Ctx = nil
	row.Cancel = nil
	log.Debug().Msgf("api.status.stop -> status.commands[%d] == %+v", commandId, *row)

	callback, payload := status.finishLocked(row)
	status.Unlock()

	// Fired outside the lock and asynchronously, a slow or broken callback
	// receiver must never stall or fail the command which just finished.
	if callback != nil {
		go notify(callback, payload)
	}
}

// finishLocked builds the completion callback for a row which reached a terminal
// state and clears it, so a command notifies at most once. It returns a nil
// config when the row has no callback or was already notified. Lock MUST be held.
func (status *AsyncStatus) finishLocked(row *ActionRow) (*CallbackConfig, CallbackPayload) {
	callback := row.callback
	if callback == nil {
		return nil, CallbackPayload{}
	}
	row.callback = nil
	return callback, CallbackPayload{
		Status:      row.Status,
		Error:       row.Error,
		OperationId: row.OperationId,
		Command:     row.Command,
		Duration:    time.Since(row.startedAt).String(),
	}
}

// closeDoneLocked closes row.Done idempotently. Must be called with the
// AsyncStatus lock held.
func closeDoneLocked(row *ActionRow) {
	if row.Done == nil {
		return
	}
	select {
	case <-row.Done:
		// already closed
	default:
		close(row.Done)
	}
}

func (status *AsyncStatus) Cancel(command string, err error) (string, error) {
	status.Lock()
	if len(status.commands) == 0 {
		status.Unlock()
		err = errors.New("empty command list")
		log.Warn().Err(err).Send()
		return "", err
	}
	commandId := -1
	if command == "" {
		for i, cmd := range status.commands {
			if cmd.Status == InProgressStatus {
				commandId = i
				break
			}
		}
	} else {
		for i, cmd := range status.commands {
			if cmd.Command == command && cmd.Ctx != nil {
				commandId = i
				break
			}
		}
	}
	if commandId == -1 {
		status.Unlock()
		err = errors.Errorf("command `%s` not found", command)
		log.Warn().Err(err).Send()
		return "", err
	}
	if status.commands[commandId].Status != InProgressStatus {
		log.Warn().Msgf("found `%s` with status=%s", command, status.commands[commandId].Status)
	}
	if status.commands[commandId].Ctx != nil {
		status.commands[commandId].Cancel()
		status.commands[commandId].Ctx = nil
		status.commands[commandId].Cancel = nil
	}
	status.commands[commandId].Error = err.Error()
	status.commands[commandId].Status = CancelStatus
	status.commands[commandId].Finish = time.Now().Format(common.TimeFormat)
	canceledCommand := status.commands[commandId].Command
	done := status.commands[commandId].Done
	log.Debug().Msgf("api.status.cancel -> status.commands[%d] == %+v", commandId, status.commands[commandId])
	status.Unlock()
	waitDone(done, canceledCommand)
	return canceledCommand, nil
}

// waitDone blocks until the command goroutine signals completion via its
// Done channel, or until CancelWaitTimeout elapses. Lock MUST NOT be held.
func waitDone(done chan struct{}, command string) {
	if done == nil {
		return
	}
	select {
	case <-done:
		return
	default:
	}
	log.Info().Msgf("status.Cancel: waiting up to %s for command %q goroutine to finish", CancelWaitTimeout, command)
	select {
	case <-done:
	case <-time.After(CancelWaitTimeout):
		log.Warn().Msgf("status.Cancel: timeout (%s) waiting for command %q goroutine to finish", CancelWaitTimeout, command)
	}
}

func (status *AsyncStatus) CancelAll(cancelMsg string) []string {
	status.Lock()
	canceled := make([]string, 0, len(status.commands))
	dones := make([]chan struct{}, 0, len(status.commands))
	for commandId := range status.commands {
		wasInProgress := status.commands[commandId].Status == InProgressStatus
		if status.commands[commandId].Ctx != nil {
			status.commands[commandId].Cancel()
			status.commands[commandId].Ctx = nil
			status.commands[commandId].Cancel = nil
		}
		if wasInProgress {
			canceled = append(canceled, status.commands[commandId].Command)
			if status.commands[commandId].Done != nil {
				dones = append(dones, status.commands[commandId].Done)
			}
		}
		status.commands[commandId].Status = CancelStatus
		status.commands[commandId].Error = cancelMsg
		status.commands[commandId].Finish = time.Now().Format(common.TimeFormat)
		log.Debug().Msgf("api.status.cancel -> status.commands[%d] == %+v", commandId, status.commands[commandId])
	}
	status.Unlock()
	for i, done := range dones {
		waitDone(done, canceled[i])
	}
	return canceled
}

func (status *AsyncStatus) GetStatus(current bool, filter string, last int) []ActionRowStatus {
	status.RLock()
	defer status.RUnlock()
	if current {
		last = 1
	}
	l := len(status.commands)
	if l == 0 {
		return make([]ActionRowStatus, 0)
	}

	filteredCommands := make([]ActionRowStatus, 0)
	for _, command := range status.commands {
		if filter == "" || (strings.Contains(command.Command, filter) || strings.Contains(command.Status, filter) || strings.Contains(command.Error, filter)) {
			// copy without context and cancel
			filteredCommands = append(filteredCommands, ActionRowStatus{
				Command:     command.Command,
				Status:      command.Status,
				Start:       command.Start,
				Finish:      command.Finish,
				Error:       command.Error,
				OperationId: command.OperationId,
				Result:      command.Result,
			})
		}
	}
	if len(filteredCommands) == 0 {
		return filteredCommands
	}

	begin, end := 0, 1
	l = len(filteredCommands)
	if last > 0 && l > last {
		begin = l - last
		end = l
	} else {
		begin = 0
		end = l
	}
	return filteredCommands[begin:end]
}

func (status *AsyncStatus) GetStatusByOperationId(operationId string) []ActionRowStatus {
	status.RLock()
	defer status.RUnlock()

	for _, command := range status.commands {
		if command.OperationId == operationId {
			return []ActionRowStatus{{
				Command:     command.Command,
				Status:      command.Status,
				Start:       command.Start,
				Finish:      command.Finish,
				Error:       command.Error,
				OperationId: command.OperationId,
				Result:      command.Result,
			}}
		}
	}
	return make([]ActionRowStatus, 0)
}

// ResetAPIServerModeForTest clears the API server marker. Tests only.
func ResetAPIServerModeForTest() {
	apiServerMode.Store(false)
}
