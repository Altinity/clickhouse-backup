package clickhouse

import (
	"context"
	"errors"
	"testing"
)

type testVersionGetterOpt func(sd *testVersionGetter)

type testVersionGetter struct {
	version    int
	versionErr error
}

func newTestVersionGetter(opts ...testVersionGetterOpt) *testVersionGetter {
	v := &testVersionGetter{}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

func withVersion(version int) testVersionGetterOpt {
	return func(v *testVersionGetter) {
		v.version = version
	}
}

func withVersionErr(err error) testVersionGetterOpt {
	return func(v *testVersionGetter) {
		v.versionErr = err
	}
}

func (v *testVersionGetter) GetVersion(_ context.Context) (int, error) {
	if v.versionErr != nil {
		return -1, v.versionErr
	}
	return v.version, nil
}

func TestCanShardOperation(t *testing.T) {
	ctx := context.Background()

	t.Run("test error on version retrieval",
		func(t *testing.T) {
			v := newTestVersionGetter(withVersionErr(errors.New("error")))
			if err := canShardOperation(ctx, v); err == nil {
				t.Fatal("expected error when getting shard determiner error on version retrieval")
			}
		},
	)

	t.Run("test version too low",
		func(t *testing.T) {
			v := newTestVersionGetter(withVersion(-1))
			err := canShardOperation(ctx, v)
			if err == nil {
				t.Fatal("expected error when version number is too low")
			}
			if !errors.Is(err, ErrShardOperationVers) {
				t.Fatalf("expected ErrShardOperationUnsupported, got %v", err)
			}
		},
	)

	t.Run("test version should be OK",
		func(t *testing.T) {
			v := newTestVersionGetter(withVersion(minVersShardOp))
			if err := canShardOperation(ctx, v); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		},
	)
}
