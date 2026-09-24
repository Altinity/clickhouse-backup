package casstorage

import "testing"

func TestReconstructAbsoluteKey(t *testing.T) {
	cases := []struct {
		name, prefix, rel, want string
	}{
		{"plain", "cas/c1/blob/", "aa/abc", "cas/c1/blob/aa/abc"},
		{"leading slash on rel stripped", "cas/c1/blob/", "/aa/abc", "cas/c1/blob/aa/abc"},
		{"prefix without trailing slash idempotent", "cas/c1/blob", "aa/abc", "cas/c1/blob/aa/abc"},
		{"deep prefix", "backup/cluster/0/cas/", "metadata/foo/bar.json", "backup/cluster/0/cas/metadata/foo/bar.json"},
		{"empty rel handled", "cas/c1/blob/", "", "cas/c1/blob/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := reconstructAbsoluteKey(c.prefix, c.rel); got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}
