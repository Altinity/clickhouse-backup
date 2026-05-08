package cas

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
)

// StatusReport is the result of a LIST-only bucket health check.
type StatusReport struct {
	BackupCount         int              `json:"backup_count"`
	BlobCount           int              `json:"blob_count"`
	BlobBytes           int64            `json:"blob_bytes"`
	PruneMarker         *PruneMarkerInfo `json:"prune_marker,omitempty"`
	InProgressFresh     []InProgressInfo `json:"in_progress_fresh"`
	InProgressAbandoned []InProgressInfo `json:"in_progress_abandoned"`
	Backups             []BackupSummary  `json:"backups"`
}

// BackupSummary holds minimal per-backup metadata collected during Status.
type BackupSummary struct {
	Name       string    `json:"name"`
	UploadedAt time.Time `json:"uploaded_at"` // ModTime of metadata.json
}

// PruneMarkerInfo holds metadata about the prune.marker object.
type PruneMarkerInfo struct {
	Path       string        `json:"path"`
	ModTime    time.Time     `json:"mod_time"`
	Age        time.Duration `json:"-"`
	AgeSeconds float64       `json:"age_seconds"`
}

// InProgressInfo holds metadata about an inprogress marker object.
type InProgressInfo struct {
	Backup     string        `json:"backup"`
	ModTime    time.Time     `json:"mod_time"`
	Age        time.Duration `json:"-"`
	AgeSeconds float64       `json:"age_seconds"`
}

// Status performs a LIST-only bucket health summary for the given cluster.
// No object bodies are fetched; only metadata returned by Walk/StatFile is used.
func Status(ctx context.Context, b Backend, cfg Config) (*StatusReport, error) {
	cp := cfg.ClusterPrefix()
	r := &StatusReport{}

	// 1. Enumerate backups: walk cas/<c>/metadata/ recursively and collect
	//    entries whose key ends in /metadata.json.
	metaPrefix := cp + "metadata/"
	if err := b.Walk(ctx, metaPrefix, true, func(f RemoteFile) error {
		if !strings.HasSuffix(f.Key, "/metadata.json") {
			return nil
		}
		// Strip prefix and "/metadata.json" suffix to extract backup name.
		inner := strings.TrimPrefix(f.Key, metaPrefix)
		// inner is "<backup>/metadata.json" (possibly deeper, but we only want
		// the first path component as the backup name).
		name := strings.TrimSuffix(inner, "/metadata.json")
		// Reject paths with extra slashes (sub-dirs of a backup dir are not
		// top-level metadata.json entries).
		if strings.Contains(name, "/") {
			return nil
		}
		r.Backups = append(r.Backups, BackupSummary{
			Name:       name,
			UploadedAt: f.ModTime,
		})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cas status: walk metadata: %w", err)
	}

	// Sort backups newest-first.
	sort.Slice(r.Backups, func(i, j int) bool {
		return r.Backups[i].UploadedAt.After(r.Backups[j].UploadedAt)
	})
	r.BackupCount = len(r.Backups)

	// 2. Count blobs and sum sizes.
	blobPrefix := cp + "blob/"
	if err := b.Walk(ctx, blobPrefix, true, func(f RemoteFile) error {
		r.BlobCount++
		r.BlobBytes += f.Size
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cas status: walk blobs: %w", err)
	}

	// 3. Check prune marker.
	pruneKey := PruneMarkerPath(cp)
	_, modTime, exists, err := b.StatFile(ctx, pruneKey)
	if err != nil {
		return nil, fmt.Errorf("cas status: stat prune marker: %w", err)
	}
	if exists {
		age := time.Since(modTime)
		r.PruneMarker = &PruneMarkerInfo{
			Path:       pruneKey,
			ModTime:    modTime,
			Age:        age,
			AgeSeconds: age.Seconds(),
		}
	}

	// 4. Classify in-progress markers.
	ipPrefix := cp + "inprogress/"
	now := time.Now()
	if err := b.Walk(ctx, ipPrefix, true, func(f RemoteFile) error {
		if !strings.HasSuffix(f.Key, ".marker") {
			return nil
		}
		// Extract backup name: strip prefix and ".marker" suffix.
		inner := strings.TrimPrefix(f.Key, ipPrefix)
		backup := strings.TrimSuffix(inner, ".marker")
		age := now.Sub(f.ModTime)
		info := InProgressInfo{
			Backup:     backup,
			ModTime:    f.ModTime,
			Age:        age,
			AgeSeconds: age.Seconds(),
		}
		if age >= cfg.AbandonThresholdDuration() {
			r.InProgressAbandoned = append(r.InProgressAbandoned, info)
		} else {
			r.InProgressFresh = append(r.InProgressFresh, info)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cas status: walk inprogress: %w", err)
	}

	// Sort InProgressFresh and InProgressAbandoned by backup name.
	sort.Slice(r.InProgressFresh, func(i, j int) bool {
		return r.InProgressFresh[i].Backup < r.InProgressFresh[j].Backup
	})
	sort.Slice(r.InProgressAbandoned, func(i, j int) bool {
		return r.InProgressAbandoned[i].Backup < r.InProgressAbandoned[j].Backup
	})

	return r, nil
}

// PrintStatus writes a human-readable summary of r to w.
func PrintStatus(r *StatusReport, w io.Writer) error {
	// Backup summary line.
	backupDetail := "none"
	if r.BackupCount > 0 {
		newest := r.Backups[0].Name
		oldest := r.Backups[r.BackupCount-1].Name
		backupDetail = fmt.Sprintf("newest: %s, oldest: %s", newest, oldest)
	}
	if _, err := fmt.Fprintf(w, "  Backups: %d (%s)\n", r.BackupCount, backupDetail); err != nil {
		return err
	}

	// Blob summary line.
	blobSize := utils.FormatBytes(uint64(r.BlobBytes))
	if _, err := fmt.Fprintf(w, "  Blobs:   %s objects, %s\n", formatInt(r.BlobCount), blobSize); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}

	// Prune marker.
	pruneStr := "NONE"
	if r.PruneMarker != nil {
		pruneStr = fmt.Sprintf("%s (age: %s)", r.PruneMarker.Path, r.PruneMarker.Age.Round(time.Second))
	}
	if _, err := fmt.Fprintf(w, "  Prune marker: %s\n", pruneStr); err != nil {
		return err
	}

	// In-progress markers.
	if _, err := fmt.Fprintf(w, "  In-progress markers: %d fresh, %d abandoned\n",
		len(r.InProgressFresh), len(r.InProgressAbandoned)); err != nil {
		return err
	}
	for _, ip := range r.InProgressFresh {
		if _, err := fmt.Fprintf(w, "    fresh: %s (%s ago)\n",
			ip.Backup, ip.Age.Round(time.Second)); err != nil {
			return err
		}
	}
	for _, ip := range r.InProgressAbandoned {
		if _, err := fmt.Fprintf(w, "    abandoned: %s (%s ago)\n",
			ip.Backup, ip.Age.Round(time.Second)); err != nil {
			return err
		}
	}
	return nil
}

// formatInt formats an integer with comma separators (e.g. 42318 → "42,318").
func formatInt(n int) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		s = s[1:]
	}
	// Insert commas every 3 digits from the right.
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	if n < 0 {
		return "-" + string(result)
	}
	return string(result)
}
