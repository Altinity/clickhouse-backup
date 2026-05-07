package cas

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if c.Enabled {
		t.Error("default Enabled should be false")
	}
	if c.RootPrefix != "cas/" {
		t.Errorf("RootPrefix: got %q", c.RootPrefix)
	}
	if c.InlineThreshold != 524288 {
		t.Errorf("InlineThreshold: got %d", c.InlineThreshold)
	}
	if c.GraceBlob != 24*time.Hour {
		t.Errorf("GraceBlob: got %v", c.GraceBlob)
	}
	if c.AbandonThreshold != 7*24*time.Hour {
		t.Errorf("AbandonThreshold: got %v", c.AbandonThreshold)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("disabled default must validate: %v", err)
	}
}

func validEnabled() Config {
	c := DefaultConfig()
	c.Enabled = true
	c.ClusterID = "prod-1"
	return c
}

func TestValidate_HappyPath(t *testing.T) {
	if err := validEnabled().Validate(); err != nil {
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

func TestValidate_RejectsBadClusterID(t *testing.T) {
	for _, bad := range []string{"a/b", "a b", "a\tb", "a\\b", "a\nb"} {
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
	c.GraceBlob = 0
	if err := c.Validate(); err == nil {
		t.Error("zero grace must fail")
	}
	c = validEnabled()
	c.AbandonThreshold = 0
	if err := c.Validate(); err == nil {
		t.Error("zero abandon must fail")
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
