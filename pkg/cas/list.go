package cas

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
)

// CASListEntry is the user-facing summary of one CAS backup, surfaced by
// `clickhouse-backup list remote` so operators can see CAS backups alongside
// v1 backups. It is intentionally a thin DTO — full validation and sizing
// belongs to cas-status / cas-verify.
type CASListEntry struct {
	// Name is the CAS backup name (the directory segment under
	// cas/<cluster>/metadata/).
	Name string

	// UploadedAt is taken from the CAS metadata.json's UploadedAt field
	// when parseable; otherwise the metadata object's mod time. Used for
	// stable descending sort in the list output.
	UploadedAt time.Time

	// SizeBytes is the rolled-up `bytes` field from metadata.json (the
	// logical size of the source backup), if present. Zero when the
	// metadata cannot be parsed.
	SizeBytes int64

	// Description is the tag rendered in the v1 list-remote output. The
	// "[CAS]" prefix is what makes the row distinguishable from a v1
	// backup; downstream callers may append a status tag (e.g. "broken").
	Description string
}

// ListRemoteCAS walks cas/<cluster>/metadata/<backup>/metadata.json and
// returns one entry per backup. When CAS is disabled this is a no-op
// returning (nil, nil).
//
// Errors from individual metadata.json reads do NOT abort the listing — the
// affected entry is still emitted with Description "[CAS] (broken: <reason>)"
// so the operator sees the partial state. Only Walk-level errors (failure to
// enumerate the metadata/ subtree at all) propagate.
func ListRemoteCAS(ctx context.Context, b Backend, cfg Config) ([]CASListEntry, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cp := cfg.ClusterPrefix()
	metadataPrefix := cp + "metadata/"

	// Collect candidate metadata.json keys first; we read them in a
	// second pass so we don't hold the Walk callback open across remote
	// reads (some backends serialize calls on the same connection).
	//
	// Backends differ in whether the Key surfaced by Walk is absolute
	// (fakedst) or prefix-stripped (the casstorage adapter, which uses
	// rf.Name()). Suffix/HasPrefix matching plus TrimPrefix tolerates
	// both forms; we reconstruct the absolute key for the subsequent
	// GetFile call from the parsed backup name.
	type cand struct {
		name    string
		modTime time.Time
	}
	var candidates []cand
	err := b.Walk(ctx, metadataPrefix, true, func(rf RemoteFile) error {
		if !strings.HasSuffix(rf.Key, "/metadata.json") {
			return nil
		}
		rest := strings.TrimPrefix(rf.Key, metadataPrefix)
		rest = strings.TrimSuffix(rest, "/metadata.json")
		// Only direct children: cas/<cluster>/metadata/<backup>/metadata.json.
		// Anything deeper (table metadata, parts) is not a backup root.
		if rest == "" || strings.Contains(rest, "/") {
			return nil
		}
		candidates = append(candidates, cand{name: rest, modTime: rf.ModTime})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cas: list remote walk %s: %w", metadataPrefix, err)
	}

	entries := make([]CASListEntry, 0, len(candidates))
	for _, c := range candidates {
		entry := CASListEntry{
			Name:        c.name,
			UploadedAt:  c.modTime,
			Description: "[CAS]",
		}
		// Parse metadata.json to refine UploadedAt and recover the
		// logical bytes. Failures degrade the entry to "broken" but
		// never drop it from the list.
		absKey := MetadataJSONPath(cp, c.name)
		r, openErr := b.GetFile(ctx, absKey)
		if openErr != nil {
			entry.Description = fmt.Sprintf("[CAS] (broken: open metadata.json: %v)", openErr)
			entries = append(entries, entry)
			continue
		}
		body, readErr := io.ReadAll(r)
		_ = r.Close()
		if readErr != nil {
			entry.Description = fmt.Sprintf("[CAS] (broken: read metadata.json: %v)", readErr)
			entries = append(entries, entry)
			continue
		}
		var bm metadata.BackupMetadata
		if jsonErr := json.Unmarshal(body, &bm); jsonErr != nil {
			entry.Description = fmt.Sprintf("[CAS] (broken: parse metadata.json: %v)", jsonErr)
			entries = append(entries, entry)
			continue
		}
		if !bm.CreationDate.IsZero() {
			entry.UploadedAt = bm.CreationDate
		}
		// CAS metadata.json's CreationDate is the upload moment; the
		// rolled-up logical size is the sum of per-class sizes from
		// the v1 schema (CAS only ever populates the data/metadata
		// fields, but tolerate any combination here).
		entry.SizeBytes = int64(bm.DataSize + bm.MetadataSize + bm.RBACSize + bm.ConfigSize + bm.NamedCollectionsSize)
		// Distinguish v1 metadata.json that happens to live under cas/
		// (defensive — should not happen) from real CAS metadata.
		if bm.CAS == nil {
			entry.Description = "[CAS] (broken: missing cas params)"
		}
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].UploadedAt.After(entries[j].UploadedAt)
	})
	return entries, nil
}
