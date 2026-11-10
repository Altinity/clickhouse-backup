package storage

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestKnownHostsAlgorithms(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "known_hosts")
	content := "[box.example]:23 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n" +
		"[box.example]:23 ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC6Mn1Pm6Vb2pWj1qhVGTPmkfB+VlRVbbyIA/sm0mtZOKvp5u0dKuOjc1gT0q6+JnkZ+PAgm3RP0pNC1mjIgkRgdWX8kKVuj3fy6/4+r+/KmS0j5yv0aQBUsBNG+SyexQBbyVN/zMjRfgO3oc6BuDZn4PZjnEVQaJmVdM3QDGXc0w==\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := knownHostsAlgorithms(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{ssh.KeyAlgoED25519, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("# nothing\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := knownHostsAlgorithms(empty); err == nil {
		t.Fatal("expected error for a file without host keys")
	}
}
