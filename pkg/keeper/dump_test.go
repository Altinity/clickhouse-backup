package keeper

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalkDumpFile(t *testing.T) {
	dumpFile := path.Join(t.TempDir(), "replicated.jsonl")
	// first two lines are the current binary format, the third one is the legacy DumpNodeString format
	content := `{"path":"","value":null}
{"path":"uuid/2d449952-fca4-c9f2-2949-b83880124bbc","value":"QVRUQUNIIFVTRVIgdGVzdDsK"}
{"path":"U/test","value":"2d449952-fca4-c9f2-2949-b83880124bbc"}

`
	require.NoError(t, os.WriteFile(dumpFile, []byte(content), 0600))

	nodes := make([]DumpNode, 0)
	require.NoError(t, WalkDumpFile(dumpFile, func(node DumpNode) error {
		nodes = append(nodes, node)
		return nil
	}))

	require.Len(t, nodes, 3)
	assert.Equal(t, "", nodes[0].Path)
	assert.Empty(t, nodes[0].Value)
	assert.Equal(t, "uuid/2d449952-fca4-c9f2-2949-b83880124bbc", nodes[1].Path)
	assert.Equal(t, "ATTACH USER test;\n", string(nodes[1].Value))
	// legacy format value must be decoded as a plain string, not silently dropped
	assert.Equal(t, "U/test", nodes[2].Path)
	assert.Equal(t, "2d449952-fca4-c9f2-2949-b83880124bbc", string(nodes[2].Value))
}

func TestWalkDumpFileBrokenLine(t *testing.T) {
	dumpFile := path.Join(t.TempDir(), "broken.jsonl")
	require.NoError(t, os.WriteFile(dumpFile, []byte("this is not a json\n"), 0600))

	err := WalkDumpFile(dumpFile, func(node DumpNode) error { return nil })
	assert.Error(t, err)
}
