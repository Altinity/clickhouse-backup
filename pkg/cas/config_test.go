package cas

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if c.Enabled {
		t.Error("default Enabled should be false")
	}
	if c.RootPrefix != "cas/" {
		t.Errorf("RootPrefix: got %q", c.RootPrefix)
	}
	if c.InlineThreshold != 262144 {
		t.Errorf("InlineThreshold: got %d", c.InlineThreshold)
	}
	if c.GraceBlob != "24h" {
		t.Errorf("GraceBlob: got %q want \"24h\"", c.GraceBlob)
	}
	if c.AbandonThreshold != "168h" {
		t.Errorf("AbandonThreshold: got %q want \"168h\"", c.AbandonThreshold)
	}
	if c.SkipConditionalPutProbe {
		t.Error("default SkipConditionalPutProbe should be false")
	}
	if err := c.Validate(); err != nil {
		t.Errorf("disabled default must validate: %v", err)
	}
}

func TestValidate_PopulatesParsedDurations(t *testing.T) {
	c := validEnabled()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.GraceBlobDuration() != 24*time.Hour {
		t.Errorf("GraceBlobDuration: got %v want 24h", c.GraceBlobDuration())
	}
	if c.AbandonThresholdDuration() != 7*24*time.Hour {
		t.Errorf("AbandonThresholdDuration: got %v want 168h", c.AbandonThresholdDuration())
	}
}

func TestValidate_RejectsUnparseableDuration(t *testing.T) {
	c := validEnabled()
	c.GraceBlob = "not-a-duration"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "grace_blob") {
		t.Fatalf("want grace_blob parse error, got %v", err)
	}
	c = validEnabled()
	c.AbandonThreshold = "8 days" // ParseDuration doesn't accept this
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "abandon_threshold") {
		t.Fatalf("want abandon_threshold parse error, got %v", err)
	}
}

func validEnabled() Config {
	c := DefaultConfig()
	c.Enabled = true
	c.ClusterID = "prod-1"
	return c
}

func TestValidate_HappyPath(t *testing.T) {
	c := validEnabled()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidate_RejectsEmptyClusterID(t *testing.T) {
	c := validEnabled()
	c.ClusterID = ""
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "cluster_id") {
		t.Fatalf("want cluster_id error, got %v", err)
	}
}

func TestValidate_RejectsBadRootPrefix(t *testing.T) {
	for _, bad := range []string{"", "cas/../escape/", "/abs/path/", "..", "/cas/",
		// Multi-segment root_prefix would escape v1 list/retention/clean-broken
		// protection (the depth-0 BackupList walk emits single-segment names).
		"backups/cas/", "a/b/c/", "deep/cas",
	} {
		c := validEnabled()
		c.RootPrefix = bad
		if err := c.Validate(); err == nil {
			t.Errorf("expected error for RootPrefix=%q", bad)
		}
	}
}

func TestValidate_RejectsBadClusterID(t *testing.T) {
	for _, bad := range []string{"a/b", "a b", "a\tb", "a\\b", "a\nb", "..", "../escape", "a..b"} {
		c := validEnabled()
		c.ClusterID = bad
		if err := c.Validate(); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

func TestValidate_RejectsBadInlineThreshold(t *testing.T) {
	c := validEnabled()
	c.InlineThreshold = 0
	if err := c.Validate(); err == nil {
		t.Error("zero must fail")
	}
	c.InlineThreshold = MaxInline + 1
	if err := c.Validate(); err == nil {
		t.Error("> MaxInline must fail")
	}
}

func TestValidate_RejectsBadDurations(t *testing.T) {
	c := validEnabled()
	c.GraceBlob = "0s"
	if err := c.Validate(); err == nil {
		t.Error("zero grace must fail")
	}
	c = validEnabled()
	c.AbandonThreshold = "0s"
	if err := c.Validate(); err == nil {
		t.Error("zero abandon must fail")
	}
	c = validEnabled()
	c.GraceBlob = "-1h"
	if err := c.Validate(); err == nil {
		t.Error("negative grace must fail")
	}
}

// TestCASConfig_DurationYAML pins the requirement that yaml.v3 can parse
// human-readable strings like "24h" into the duration fields. With the
// previous time.Duration type, yaml deserialized as raw nanoseconds and
// any operator following the documented "grace_blob: 24h" syntax would
// silently get the wrong value (or a parse error).
func TestCASConfig_DurationYAML(t *testing.T) {
	type Outer struct {
		CAS Config `yaml:"cas"`
	}
	src := []byte(`
cas:
  enabled: true
  cluster_id: test
  root_prefix: cas/
  inline_threshold: 524288
  grace_blob: "12h"
  abandon_threshold: "72h"
`)
	var got Outer
	if err := yaml.Unmarshal(src, &got); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if got.CAS.GraceBlob != "12h" {
		t.Errorf("GraceBlob: got %q want \"12h\"", got.CAS.GraceBlob)
	}
	if got.CAS.AbandonThreshold != "72h" {
		t.Errorf("AbandonThreshold: got %q want \"72h\"", got.CAS.AbandonThreshold)
	}
	if err := got.CAS.Validate(); err != nil {
		t.Fatalf("Validate after yaml unmarshal: %v", err)
	}
	if got.CAS.GraceBlobDuration() != 12*time.Hour {
		t.Errorf("parsed grace: got %v want 12h", got.CAS.GraceBlobDuration())
	}
	if got.CAS.AbandonThresholdDuration() != 72*time.Hour {
		t.Errorf("parsed abandon: got %v want 72h", got.CAS.AbandonThresholdDuration())
	}
}

func TestClusterPrefix(t *testing.T) {
	c := validEnabled()
	if got := c.ClusterPrefix(); got != "cas/prod-1/" {
		t.Errorf("got %q want %q", got, "cas/prod-1/")
	}
	c.RootPrefix = "cas" // missing trailing slash
	if got := c.ClusterPrefix(); got != "cas/prod-1/" {
		t.Errorf("normalized: got %q want %q", got, "cas/prod-1/")
	}
}

// TestSkipPrefixes_DisabledStillProtects encodes the requirement that
// v1 retention/list operations must continue to skip the CAS namespace
// even when cas.enabled=false. Otherwise a config rollback or downgrade
// would silently expose existing CAS data to v1 deletion.
func TestSkipPrefixes_DisabledStillProtects(t *testing.T) {
	c := DefaultConfig()
	c.Enabled = false
	c.RootPrefix = "cas/"
	got := c.SkipPrefixes()
	if len(got) != 1 || got[0] != "cas/" {
		t.Errorf("disabled SkipPrefixes: got %v want [cas/]", got)
	}
}

func TestSkipPrefixes_NormalizesTrailingSlash(t *testing.T) {
	c := DefaultConfig()
	c.RootPrefix = "cas" // no trailing slash
	got := c.SkipPrefixes()
	if len(got) != 1 || got[0] != "cas/" {
		t.Errorf("got %v want [cas/]", got)
	}
}

func TestSkipPrefixes_EmptyRootPrefixReturnsNil(t *testing.T) {
	c := DefaultConfig()
	c.RootPrefix = ""
	if got := c.SkipPrefixes(); got != nil {
		t.Errorf("empty RootPrefix should return nil, got %v", got)
	}
}

func TestCASConfig_WaitForPruneParses(t *testing.T) {
	c := validEnabled()
	c.WaitForPrune = "5m"
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := c.WaitForPruneDuration(); got != 5*time.Minute {
		t.Errorf("WaitForPruneDuration: got %v want 5m", got)
	}
}

func TestCASConfig_WaitForPruneDefaultsZero(t *testing.T) {
	c := validEnabled()
	// WaitForPrune is intentionally absent / empty string
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := c.WaitForPruneDuration(); got != 0 {
		t.Errorf("WaitForPruneDuration: got %v want 0", got)
	}
}

func TestCASConfig_WaitForPruneRejectsBadDuration(t *testing.T) {
	c := validEnabled()
	c.WaitForPrune = "banana"
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for bad duration, got nil")
	}
	if !strings.Contains(err.Error(), "wait_for_prune") {
		t.Errorf("error should mention wait_for_prune, got: %v", err)
	}
}
