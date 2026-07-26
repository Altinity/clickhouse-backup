package backup

import (
	"context"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// startWatchIteration registers a per-iteration status row and returns a finish
// function that Stops the row and dispatches general.callback_url. Parent ctx
// cancellation is bridged so mid-iteration kills do not leave "in progress" rows.
func (b *Backuper) startWatchIteration(parentCtx context.Context, command string) (iterCommandId int, operationId string, start time.Time, finish func(error)) {
	opUUID, err := uuid.NewUUID()
	if err != nil {
		operationId = ""
	} else {
		operationId = opUUID.String()
	}
	start = time.Now()
	iterCommandId, _ = status.Current.StartWithOperationId(command, operationId)
	_, iterCancel, ctxErr := status.Current.GetContextWithCancel(iterCommandId)
	if ctxErr != nil {
		iterCancel = func() {}
	}
	bridgeDone := make(chan struct{})
	go func() {
		select {
		case <-parentCtx.Done():
			iterCancel()
		case <-bridgeDone:
		}
	}()

	finished := false
	finish = func(cycleErr error) {
		if finished {
			return
		}
		finished = true
		close(bridgeDone)
		status.Current.Stop(iterCommandId, cycleErr)
		b.dispatchWatchCallback(command, operationId, start, cycleErr)
	}
	return iterCommandId, operationId, start, finish
}

func (b *Backuper) dispatchWatchCallback(command, operationId string, start time.Time, cycleErr error) {
	if b.cfg == nil || b.cfg.General.CallbackURL == "" {
		return
	}
	payload := status.CallbackPayload{
		Command:     command,
		Duration:    time.Since(start).String(),
		OperationId: operationId,
	}
	if cycleErr != nil {
		payload.Status = status.ErrorStatus
		payload.Error = cycleErr.Error()
	} else {
		payload.Status = status.SuccessStatus
		payload.Error = ""
	}
	timeout := b.cfg.General.CallbackTimeoutDuration
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if cbErr := status.SendCallback(ctx, b.cfg.General.CallbackURL, payload); cbErr != nil {
		log.Error().Err(cbErr).Str("callback_url", b.cfg.General.CallbackURL).Msg("watch callback failed")
	}
}

func watchCycleError(createRemoteErr, deleteLocalErr error) error {
	if createRemoteErr != nil {
		return createRemoteErr
	}
	return deleteLocalErr
}
