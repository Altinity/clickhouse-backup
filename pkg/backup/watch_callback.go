package backup

import (
	"context"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// startWatchIteration returns a unique operation ID and an idempotent finish callback.
//
// Iterations deliberately bypass status.Current to avoid unbounded memory growth in
// status.commands over long-running watch processes. Cancellation and progress are
// instead tracked via the top-level watch command context.
func (b *Backuper) startWatchIteration(command string) (string, func(error)) {
	operationID := uuid.NewString()
	start := time.Now()
	var once sync.Once

	finish := func(cycleErr error) {
		once.Do(func() {
			b.dispatchWatchCallback(command, operationID, start, cycleErr)
		})
	}

	return operationID, finish
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
