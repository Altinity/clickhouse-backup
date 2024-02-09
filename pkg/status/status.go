package status

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	apexLog "github.com/apex/log"
	"strings"
	"sync"
	"time"
)

const (
	InProgressStatus = "in progress"
	SuccessStatus    = "success"
	CancelStatus     = "cancel"
	ErrorStatus      = "error"
)

var Current = &AsyncStatus{
	log: apexLog.WithField("logger", "status"),
}

const NotFromAPI = int(-1)

type AsyncStatus struct {
	commands []ActionRow
	log      *apexLog.Entry
	sync.RWMutex
}

type ActionRowStatus struct {
	Command string `json:"command"`
	Status  string `json:"status"`
	Start   string `json:"start,omitempty"`
	Finish  string `json:"finish,omitempty"`
	Error   string `json:"error,omitempty"`
}

type ActionRow struct {
	ActionRowStatus
	Ctx    context.Context
	Cancel context.CancelFunc
}

func (status *AsyncStatus) Start(command string) (int, context.Context) {
	status.Lock()
	defer status.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	status.commands = append(status.commands, ActionRow{
		ActionRowStatus: ActionRowStatus{
			Command: command,
			Start:   time.Now().Format(common.TimeFormat),
			Status:  InProgressStatus,
		},
		Ctx:    ctx,
		Cancel: cancel,
	})
	lastCommandId := len(status.commands) - 1
	status.log.Debugf("api.status.Start -> status.commands[%d] == %+v", lastCommandId, status.commands[lastCommandId])
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
			status.log.Debugf("api.status.inProgress -> status.commands[%d].Status == %s, inProgress=%v", n, status.commands[n].Status, status.commands[n].Status == InProgressStatus)
			return true
		}
	}

	status.log.Debugf("api.status.inProgress -> len(status.commands)=%d, inProgress=false", len(status.commands))
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
		return nil, nil, fmt.Errorf("commandId=%d not exists in current running commands", commandId)
	}
	if status.commands[commandId].Ctx == nil {
		return nil, nil, fmt.Errorf("commands[%d]=%s have nil context ", commandId, status.commands[commandId].Command)
	}
	return status.commands[commandId].Ctx, status.commands[commandId].Cancel, nil
}

func (status *AsyncStatus) Stop(commandId int, err error) {
	status.Lock()
	defer status.Unlock()
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
	status.log.Debugf("api.status.stop -> status.commands[%d] == %+v", commandId, status.commands[commandId])
}

func (status *AsyncStatus) Cancel(command string, err error) error {
	status.Lock()
	defer status.Unlock()
	if len(status.commands) == 0 {
		err = fmt.Errorf("empty command list")
		status.log.Warnf(err.Error())
		return err
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
		err = fmt.Errorf("command `%s` not found", command)
		status.log.Warnf(err.Error())
		return err
	}
	if status.commands[commandId].Status != InProgressStatus {
		status.log.Warnf("found `%s` with status=%s", command, status.commands[commandId].Status)
	}
	if status.commands[commandId].Ctx != nil {
		status.commands[commandId].Cancel()
		status.commands[commandId].Ctx = nil
		status.commands[commandId].Cancel = nil
	}
	status.commands[commandId].Error = err.Error()
	status.commands[commandId].Status = CancelStatus
	status.commands[commandId].Finish = time.Now().Format(common.TimeFormat)
	status.log.Debugf("api.status.cancel -> status.commands[%d] == %+v", commandId, status.commands[commandId])
	return nil
}

func (status *AsyncStatus) CancelAll(cancelMsg string) {
	status.Lock()
	defer status.Unlock()
	for commandId := range status.commands {
		if status.commands[commandId].Ctx != nil {
			status.commands[commandId].Cancel()
			status.commands[commandId].Ctx = nil
			status.commands[commandId].Cancel = nil
		}
		status.commands[commandId].Status = CancelStatus
		status.commands[commandId].Error = cancelMsg
		status.commands[commandId].Finish = time.Now().Format(common.TimeFormat)
		status.log.Debugf("api.status.cancel -> status.commands[%d] == %+v", commandId, status.commands[commandId])
	}
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
				Command: command.Command,
				Status:  command.Status,
				Start:   command.Start,
				Finish:  command.Finish,
				Error:   command.Error,
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
