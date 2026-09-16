package cas

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config holds CAS-specific configuration. Embedded in pkg/config.Config under
// the `cas` key. See docs/cas-design.md §6.11.
//
// GraceBlob and AbandonThreshold are typed string (not time.Duration) because
// gopkg.in/yaml.v3 deserializes time.Duration as raw nanoseconds, not as
// human-readable durations like "24h". Operators expect to write
// `grace_blob: "24h"` in config.yml. Validate() parses these strings via
// time.ParseDuration and stores the result in unexported fields; runtime
// callers MUST use GraceBlobDuration() / AbandonThresholdDuration() instead
// of reading the string fields directly.
type Config struct {
	Enabled          bool   `yaml:"enabled" envconfig:"CAS_ENABLED"`
	ClusterID        string `yaml:"cluster_id" envconfig:"CAS_CLUSTER_ID"`
	RootPrefix       string `yaml:"root_prefix" envconfig:"CAS_ROOT_PREFIX"`
	InlineThreshold  uint64 `yaml:"inline_threshold" envconfig:"CAS_INLINE_THRESHOLD"`
	GraceBlob        string `yaml:"grace_blob" envconfig:"CAS_GRACE_BLOB"`
	AbandonThreshold string `yaml:"abandon_threshold" envconfig:"CAS_ABANDON_THRESHOLD"`
	WaitForPrune     string `yaml:"wait_for_prune" envconfig:"CAS_WAIT_FOR_PRUNE"`
	// AllowUnsafeMarkers, when true, lets backends without native atomic-create
	// (currently only FTP) write CAS markers using a stat-then-rename fallback
	// with a documented race window. Default false; CAS refuses marker writes
	// on those backends unless the operator explicitly opts in.
	AllowUnsafeMarkers bool `yaml:"allow_unsafe_markers" envconfig:"CAS_ALLOW_UNSAFE_MARKERS"`

	// SkipConditionalPutProbe, when true, disables the startup probe that
	// verifies the backend correctly honors If-None-Match: * (i.e. refuses to
	// overwrite an existing object via PutFileIfAbsent). The probe detects older
	// MinIO (<2024-11), older Ceph RGW, and other buggy S3-compatible stores
	// that silently ignore the precondition, defeating marker locks and risking
	// data loss. Set to true ONLY if you knowingly run on a non-conforming
	// backend and accept the risk.
	SkipConditionalPutProbe bool `yaml:"skip_conditional_put_probe" envconfig:"CAS_SKIP_CONDITIONAL_PUT_PROBE"`

	// AllowUnsafeObjectDiskSkip, when true, allows cas-upload to continue even
	// when the object-disk pre-flight cannot query system.disks (e.g. transient
	// ClickHouse unavailability) or cannot inspect table metadata JSON. By
	// default (false) any failure in the object-disk detection pipeline is a
	// hard error, ensuring CAS never silently ingests a backup that may contain
	// unrestorable object-disk-backed tables. Set to true ONLY if you cannot
	// query system.disks at upload time and consciously accept that the
	// resulting CAS backup may include object-disk tables that cannot be
	// restored.
	AllowUnsafeObjectDiskSkip bool `yaml:"allow_unsafe_object_disk_skip" envconfig:"CAS_ALLOW_UNSAFE_OBJECT_DISK_SKIP"`

	// Parsed by Validate(). Zero until Validate() runs.
	graceBlobDur        time.Duration
	abandonThresholdDur time.Duration
	waitForPruneDur     time.Duration
}

// GraceBlobDuration returns the parsed grace_blob value. Returns 0 if
// Validate() has not been called.
func (c Config) GraceBlobDuration() time.Duration { return c.graceBlobDur }

// AbandonThresholdDuration returns the parsed abandon_threshold value.
// Returns 0 if Validate() has not been called.
func (c Config) AbandonThresholdDuration() time.Duration { return c.abandonThresholdDur }

// WaitForPruneDuration returns the parsed wait_for_prune value. Returns 0 if
// Validate() has not been called or wait_for_prune was not set.
func (c Config) WaitForPruneDuration() time.Duration { return c.waitForPruneDur }

// DefaultConfig returns the safe defaults. Enabled is false by default; CAS
// is opt-in. ClusterID has no default — operators MUST set it explicitly when
// enabling CAS.
func DefaultConfig() Config {
	return Config{
		Enabled:          false,
		ClusterID:        "",
		RootPrefix:       "cas/",
		InlineThreshold:  262144, // 256 KiB
		GraceBlob:        "24h",
		AbandonThreshold: "168h", // 7 days
	}
}

// SkipPrefixes returns the prefixes that v1 list/retention must ignore. The
// returned prefixes always end with "/" so a simple HasPrefix check on a
// remote key correctly distinguishes "cas/" from a hypothetical sibling like
// "case-archive/".
//
// v1 callers pass this into BackupDestination.BackupList so the cas/<cluster>/
// subtree is not scanned (which would otherwise be reported as broken backup
// folders and might be deleted by retention or "clean remote_broken").
//
// IMPORTANT: this returns the prefix exclusion regardless of c.Enabled. If
// CAS is disabled, the operator might be in a config rollback or downgrade
// scenario where existing CAS data lives in the bucket but cas-* commands
// are off. Returning nil here would let v1 retention silently delete that
// data the next time RemoveOldBackupsRemote runs. The protection follows
// from the existence of the namespace, not from the feature being enabled.
// Returns nil only when RootPrefix is empty (no namespace to protect).
func (c Config) SkipPrefixes() []string {
	rp := c.RootPrefix
	if rp != "" && !strings.HasSuffix(rp, "/") {
		rp += "/"
	}
	if rp == "" {
		return nil
	}
	return []string{rp}
}

// ClusterPrefix returns the per-cluster prefix used for every CAS object key.
// Always ends with "/". Form: "<root_prefix><cluster_id>/", e.g. "cas/prod-1/".
//
// Callers must only use this when c.Enabled and c.Validate() has succeeded;
// otherwise the result may not satisfy the implicit "ends with /" contract
// callers depend on.
func (c Config) ClusterPrefix() string {
	rp := c.RootPrefix
	if rp != "" && !strings.HasSuffix(rp, "/") {
		rp += "/"
	}
	return rp + c.ClusterID + "/"
}

// Validate returns nil if disabled. When enabled, enforces:
//   - ClusterID is non-empty and contains no whitespace or path separators.
//   - InlineThreshold is in (0, MaxInline].
//   - GraceBlob and AbandonThreshold parse via time.ParseDuration and are
//     strictly positive. Parsed values are stored on the receiver; callers
//     access them via GraceBlobDuration() and AbandonThresholdDuration().
//
// Pointer receiver: parsed durations need to persist on the embedded
// pkg/config.Config.CAS field after pkg/config.ValidateConfig calls
// cfg.CAS.Validate().
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ClusterID == "" {
		return errors.New("cas.cluster_id is required when cas.enabled=true")
	}
	if strings.ContainsAny(c.ClusterID, "/\\ \t\n") {
		return fmt.Errorf("cas.cluster_id %q must not contain whitespace or path separators", c.ClusterID)
	}
	if strings.Contains(c.ClusterID, "..") {
		return fmt.Errorf("cas.cluster_id %q must not contain %q (path traversal)", c.ClusterID, "..")
	}
	if c.RootPrefix == "" {
		return errors.New("cas.root_prefix must not be empty when cas.enabled=true")
	}
	if strings.Contains(c.RootPrefix, "..") || strings.HasPrefix(c.RootPrefix, "/") {
		return fmt.Errorf("cas.root_prefix %q must not contain %q or start with %q", c.RootPrefix, "..", "/")
	}
	// Multi-segment root_prefix (e.g. "backups/cas/") would escape v1 list/
	// retention/clean-broken protection: BackupList walks the bucket root
	// at depth 0 and emits single-segment entries like "backups", but
	// SkipPrefixes returns "backups/cas/", so the equality/HasPrefix check
	// in pkg/storage/general.go::BackupList misses the parent directory
	// and v1 may treat the CAS parent as a broken v1 backup. v1 of CAS
	// requires a single-segment root_prefix; for nested layouts, set the
	// underlying BackupDestination path (s3.path / sftp.path / etc.) to
	// the parent and keep cas.root_prefix as a single segment.
	trimmed := strings.TrimSuffix(c.RootPrefix, "/")
	if strings.Contains(trimmed, "/") {
		return fmt.Errorf("cas.root_prefix %q must be a single path segment (e.g. \"cas/\"); for nested layouts, set the storage backend path (s3.path / sftp.path / etc.) and keep cas.root_prefix as one segment", c.RootPrefix)
	}
	if c.InlineThreshold == 0 || c.InlineThreshold > MaxInline {
		return fmt.Errorf("cas.inline_threshold must be in (0, %d], got %d", MaxInline, c.InlineThreshold)
	}
	gb, err := time.ParseDuration(c.GraceBlob)
	if err != nil {
		return fmt.Errorf("cas.grace_blob %q: %w", c.GraceBlob, err)
	}
	if gb <= 0 {
		return fmt.Errorf("cas.grace_blob must be > 0, got %v", gb)
	}
	at, err := time.ParseDuration(c.AbandonThreshold)
	if err != nil {
		return fmt.Errorf("cas.abandon_threshold %q: %w", c.AbandonThreshold, err)
	}
	if at <= 0 {
		return fmt.Errorf("cas.abandon_threshold must be > 0, got %v", at)
	}
	c.graceBlobDur = gb
	c.abandonThresholdDur = at
	if c.WaitForPrune != "" {
		wfp, err := time.ParseDuration(c.WaitForPrune)
		if err != nil {
			return fmt.Errorf("cas.wait_for_prune %q: %w", c.WaitForPrune, err)
		}
		if wfp < 0 {
			return fmt.Errorf("cas.wait_for_prune must be >= 0, got %v", wfp)
		}
		c.waitForPruneDur = wfp
	}
	return nil
}
