package cas

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"time"
)

// markerTool is embedded in marker JSON for forensic context. Set by callers
// (typically to "clickhouse-backup <version>"); empty is fine.
var markerTool = "clickhouse-backup"

// SetMarkerTool overrides the tool string written into new markers. Intended
// to be called once at startup with a version-tagged identifier.
func SetMarkerTool(tool string) { markerTool = tool }

// hostname returns the host's name; on error returns "unknown".
func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown"
	}
	return h
}

// nowRFC3339 returns the current UTC time in RFC3339 format.
func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// WriteInProgressMarker atomically creates cas/<cluster>/inprogress/<backup>.marker.
// Returns (true, nil) on successful create; (false, nil) if a marker
// already exists (another upload is in progress); (false, ErrConditionalPutNotSupported)
// when the backend can't do atomic create.
func WriteInProgressMarker(ctx context.Context, b Backend, clusterPrefix, backup, host string) (created bool, err error) {
	return WriteInProgressMarkerWithTool(ctx, b, clusterPrefix, backup, host, markerTool)
}

// WriteInProgressMarkerWithTool is like WriteInProgressMarker but accepts an
// explicit tool identifier written into the marker JSON. Use this when the
// caller is not "cas-upload" (e.g. "cas-delete") so that concurrent operations
// can surface the right diagnostic in error messages.
func WriteInProgressMarkerWithTool(ctx context.Context, b Backend, clusterPrefix, backup, host, tool string) (created bool, err error) {
	if host == "" {
		host = hostname()
	}
	if tool == "" {
		tool = markerTool
	}
	m := InProgressMarker{Backup: backup, Host: host, StartedAt: nowRFC3339(), Tool: tool}
	data, _ := json.Marshal(m)
	return b.PutFileIfAbsent(ctx, InProgressMarkerPath(clusterPrefix, backup),
		io.NopCloser(bytes.NewReader(data)), int64(len(data)))
}

// ReadInProgressMarker returns the parsed marker. Returns an error wrapping
// io.EOF (or similar) if the marker doesn't exist; callers can use StatFile
// for an exists/not-exists probe instead.
func ReadInProgressMarker(ctx context.Context, b Backend, clusterPrefix, backup string) (*InProgressMarker, error) {
	raw, err := getBytes(ctx, b, InProgressMarkerPath(clusterPrefix, backup))
	if err != nil {
		return nil, err
	}
	var m InProgressMarker
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// DeleteInProgressMarker removes the in-progress marker for the given backup.
func DeleteInProgressMarker(ctx context.Context, b Backend, clusterPrefix, backup string) error {
	return b.DeleteFile(ctx, InProgressMarkerPath(clusterPrefix, backup))
}

// WritePruneMarker atomically creates cas/<cluster>/prune.marker. Returns
// (runID, true, nil) on successful create; ("", false, nil) when another
// prune already holds the marker; ("", false, ErrConditionalPutNotSupported)
// for backends without atomic-create.
func WritePruneMarker(ctx context.Context, b Backend, clusterPrefix, host string) (runID string, created bool, err error) {
	if host == "" {
		host = hostname()
	}
	runID = randomRunID()
	m := PruneMarker{Host: host, StartedAt: nowRFC3339(), RunID: runID, Tool: markerTool}
	data, _ := json.Marshal(m)
	created, err = b.PutFileIfAbsent(ctx, PruneMarkerPath(clusterPrefix),
		io.NopCloser(bytes.NewReader(data)), int64(len(data)))
	if !created || err != nil {
		return "", created, err
	}
	return runID, true, nil
}

// ReadPruneMarker returns the parsed prune marker.
func ReadPruneMarker(ctx context.Context, b Backend, clusterPrefix string) (*PruneMarker, error) {
	raw, err := getBytes(ctx, b, PruneMarkerPath(clusterPrefix))
	if err != nil {
		return nil, err
	}
	var m PruneMarker
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// DeletePruneMarker removes the prune marker.
func DeletePruneMarker(ctx context.Context, b Backend, clusterPrefix string) error {
	return b.DeleteFile(ctx, PruneMarkerPath(clusterPrefix))
}

// --- helpers ---

func randomHex(nBytes int) (string, error) {
	buf := make([]byte, nBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// randomRunID returns a 16-hex-char random identifier. Panics only if the
// OS entropy source is completely broken (effectively impossible in practice).
func randomRunID() string {
	id, err := randomHex(8)
	if err != nil {
		panic("cas: randomRunID: entropy unavailable: " + err.Error())
	}
	return id
}

func putBytes(ctx context.Context, b Backend, key string, data []byte) error {
	return b.PutFile(ctx, key, io.NopCloser(bytes.NewReader(data)), int64(len(data)))
}

// markerSizeLimit is the maximum number of bytes we will read from a remote
// marker file. Real markers are ~200 B; 64 KiB is a safe ceiling that prevents
// a corrupt / malicious object from consuming unbounded memory.
const markerSizeLimit = 64 * 1024

func getBytes(ctx context.Context, b Backend, key string) ([]byte, error) {
	rc, err := b.GetFile(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(io.LimitReader(rc, markerSizeLimit))
}
