package status

import (
	"context"
	stderrors "errors"
	"strings"
	"sync"
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

type AsyncStatus struct {
	commands []ActionRow
	sync.RWMutex
}

type ActionRowStatus struct {
	Command     string `json:"command"`
	Status      string `json:"status"`
	Start       string `json:"start,omitempty"`
	Finish      string `json:"finish,omitempty"`
	Error       string `json:"error,omitempty"`
	OperationId string `json:"operation_id,omitempty"`
}

type ActionRow struct {
	ActionRowStatus
	Ctx    context.Context
	Cancel context.CancelFunc
	// Done is closed by Stop when the command goroutine has fully returned.
	// Cancel/CancelAll wait on this so callers know the operation really
	// finished (e.g. defers like pidlock.RemovePidFile have already run).
	Done chan struct{}
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
	status.Lock()
	defer status.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	status.commands = append(status.commands, ActionRow{
		ActionRowStatus: ActionRowStatus{
			Command:     command,
			Start:       time.Now().Format(common.TimeFormat),
			Status:      InProgressStatus,
			OperationId: operationId,
		},
		Ctx:    ctx,
		Cancel: cancel,
		Done:   make(chan struct{}),
	})
	lastCommandId := len(status.commands) - 1
	log.Debug().Msgf("api.status.Start -> status.commands[%d] == %+v", lastCommandId, status.commands[lastCommandId])
	return lastCommandId, ctx
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
	if commandId >= len(status.commands) {
		return nil, nil, errors.Errorf("commandId=%d not exists in current running commands", commandId)
	}
	if status.commands[commandId].Ctx == nil {
		return nil, nil, errors.Errorf("commands[%d]=%s have nil context ", commandId, status.commands[commandId].Command)
	}
	// for create_remote and restore_remote API call
	if stderrors.Is(status.commands[commandId].Ctx.Err(), context.Canceled) && strings.Contains(status.commands[commandId].Command, "_remote") {
		status.commands[commandId].Ctx, status.commands[commandId].Cancel = context.WithCancel(context.Background())
	}
	return status.commands[commandId].Ctx, status.commands[commandId].Cancel, nil
}

func (status *AsyncStatus) Stop(commandId int, err error) {
	status.Lock()
	defer status.Unlock()
	// Always signal "goroutine finished" to any Cancel waiter, even if the
	// row was already moved to cancel/error/success state by a concurrent
	// Cancel() call.
	closeDoneLocked(&status.commands[commandId])
	if status.commands[commandId].Status != InProgressStatus {
		return
	}
	status.commands[commandId].Cancel()
	s := SuccessStatus
	if err != nil {
		s = ErrorStatus
		status.commands[commandId].Error = err.Error()
	}
	status.commands[commandId].Status = s
	status.commands[commandId].Finish = time.Now().Format(common.TimeFormat)
	status.commands[commandId].Ctx = nil
	status.commands[commandId].Cancel = nil
	log.Debug().Msgf("api.status.stop -> status.commands[%d] == %+v", commandId, status.commands[commandId])
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
			}}
		}
	}
	return make([]ActionRowStatus, 0)
}
