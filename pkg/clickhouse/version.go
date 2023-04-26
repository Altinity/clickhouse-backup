package clickhouse

import (
	"context"
	"errors"
)

const (
	minVersShardOp = 21000000
)

var (
	ErrShardOperationVers = errors.New("sharded operations are only supported for " +
		"clickhouse-server >= v21.x")
)

type versionGetter interface {
	GetVersion(ctx context.Context) (int, error)
}

func canShardOperation(ctx context.Context, v versionGetter) error {
	version, err := v.GetVersion(ctx)
	if err != nil {
		return err
	}
	if version < minVersShardOp {
		return ErrShardOperationVers
	}
	return nil
}

func (ch *ClickHouse) CanShardOperation(ctx context.Context) error {
	return canShardOperation(ctx, ch)
}
