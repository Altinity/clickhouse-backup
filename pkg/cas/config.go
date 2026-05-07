package cas

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config holds CAS-specific configuration. Embedded in pkg/config.Config under
// the `cas` key. See docs/cas-design.md §6.11.
type Config struct {
	Enabled          bool          `yaml:"enabled" envconfig:"CAS_ENABLED"`
	ClusterID        string        `yaml:"cluster_id" envconfig:"CAS_CLUSTER_ID"`
	RootPrefix       string        `yaml:"root_prefix" envconfig:"CAS_ROOT_PREFIX"`
	InlineThreshold  uint64        `yaml:"inline_threshold" envconfig:"CAS_INLINE_THRESHOLD"`
	GraceBlob        time.Duration `yaml:"grace_blob" envconfig:"CAS_GRACE_BLOB"`
	AbandonThreshold time.Duration `yaml:"abandon_threshold" envconfig:"CAS_ABANDON_THRESHOLD"`
}

// DefaultConfig returns the safe defaults. Enabled is false by default; CAS
// is opt-in. ClusterID has no default — operators MUST set it explicitly when
// enabling CAS.
func DefaultConfig() Config {
	return Config{
		Enabled:          false,
		ClusterID:        "",
		RootPrefix:       "cas/",
		InlineThreshold:  524288, // 512 KiB
		GraceBlob:        24 * time.Hour,
		AbandonThreshold: 7 * 24 * time.Hour,
	}
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
//   - GraceBlob and AbandonThreshold are strictly positive.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ClusterID == "" {
		return errors.New("cas.cluster_id is required when cas.enabled=true")
	}
	if strings.ContainsAny(c.ClusterID, "/\\ \t\n") {
		return fmt.Errorf("cas.cluster_id %q must not contain whitespace or path separators", c.ClusterID)
	}
	if c.InlineThreshold == 0 || c.InlineThreshold > MaxInline {
		return fmt.Errorf("cas.inline_threshold must be in (0, %d], got %d", MaxInline, c.InlineThreshold)
	}
	if c.GraceBlob <= 0 {
		return errors.New("cas.grace_blob must be > 0")
	}
	if c.AbandonThreshold <= 0 {
		return errors.New("cas.abandon_threshold must be > 0")
	}
	return nil
}
