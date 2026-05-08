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
	// AllowUnsafeMarkers, when true, lets backends without native atomic-create
	// (currently only FTP) write CAS markers using a stat-then-rename fallback
	// with a documented race window. Default false; CAS refuses marker writes
	// on those backends unless the operator explicitly opts in.
	AllowUnsafeMarkers bool `yaml:"allow_unsafe_markers" envconfig:"CAS_ALLOW_UNSAFE_MARKERS"`

	// Parsed by Validate(). Zero until Validate() runs.
	graceBlobDur        time.Duration
	abandonThresholdDur time.Duration
}

// GraceBlobDuration returns the parsed grace_blob value. Returns 0 if
// Validate() has not been called.
func (c Config) GraceBlobDuration() time.Duration { return c.graceBlobDur }

// AbandonThresholdDuration returns the parsed abandon_threshold value.
// Returns 0 if Validate() has not been called.
func (c Config) AbandonThresholdDuration() time.Duration { return c.abandonThresholdDur }

// DefaultConfig returns the safe defaults. Enabled is false by default; CAS
// is opt-in. ClusterID has no default — operators MUST set it explicitly when
// enabling CAS.
func DefaultConfig() Config {
	return Config{
		Enabled:          false,
		ClusterID:        "",
		RootPrefix:       "cas/",
		InlineThreshold:  524288, // 512 KiB
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
	return nil
}
