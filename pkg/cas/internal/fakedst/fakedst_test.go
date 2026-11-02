package fakedst

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/cas"
)

func TestFake_PutGetStatDelete(t *testing.T) {
	f := New()
	ctx := context.Background()

	body := io.NopCloser(bytes.NewReader([]byte("hello")))
	if err := f.PutFile(ctx, "a/b", body, 5); err != nil {
		t.Fatal(err)
	}

	sz, _, exists, err := f.StatFile(ctx, "a/b")
	if err != nil || !exists || sz != 5 {
		t.Fatalf("stat: sz=%d exists=%v err=%v", sz, exists, err)
	}

	_, _, exists, err = f.StatFile(ctx, "missing")
	if err != nil || exists {
		t.Fatalf("stat missing: exists=%v err=%v", exists, err)
	}

	rc, err := f.GetFile(ctx, "a/b")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}

	if err := f.DeleteFile(ctx, "a/b"); err != nil {
		t.Fatal(err)
	}
	_, _, exists, _ = f.StatFile(ctx, "a/b")
	if exists {
		t.Fatal("after delete must not exist")
	}
}

func TestFake_WalkRecursive(t *testing.T) {
	f := New()
	ctx := context.Background()

	for _, k := range []string{"p/a", "p/b/c", "p/b/d", "q/e"} {
		_ = f.PutFile(ctx, k, io.NopCloser(bytes.NewReader(nil)), 0)
	}

	var got []string
	_ = f.Walk(ctx, "p/", true, func(r cas.RemoteFile) error {
		got = append(got, r.Key)
		return nil
	})
	sort.Strings(got)
	want := []string{"p/a", "p/b/c", "p/b/d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recursive: got %v want %v", got, want)
	}
}

func TestFake_WalkNonRecursive(t *testing.T) {
	f := New()
	ctx := context.Background()

	for _, k := range []string{"p/a", "p/b/c", "p/d"} {
		_ = f.PutFile(ctx, k, io.NopCloser(bytes.NewReader(nil)), 0)
	}

	var got []string
	_ = f.Walk(ctx, "p/", false, func(r cas.RemoteFile) error {
		got = append(got, r.Key)
		return nil
	})
	sort.Strings(got)
	want := []string{"p/a", "p/d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("non-recursive: got %v want %v", got, want)
	}
}

func TestFake_SetModTime(t *testing.T) {
	f := New()
	ctx := context.Background()

	_ = f.PutFile(ctx, "k", io.NopCloser(bytes.NewReader(nil)), 0)
	past := time.Now().Add(-72 * time.Hour)
	f.SetModTime("k", past)
	_, mt, _, _ := f.StatFile(ctx, "k")
	if !mt.Equal(past) {
		t.Fatalf("modtime: got %v want %v", mt, past)
	}
}
