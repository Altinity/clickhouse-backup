package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmbeddedCluster(t *testing.T) {
	assert.Equal(t, "", (&BackupMetadata{Tags: "regular"}).EmbeddedCluster())
	assert.Equal(t, "", (&BackupMetadata{Tags: "embedded"}).EmbeddedCluster())
	assert.Equal(t, "", (&BackupMetadata{}).EmbeddedCluster())
	assert.Equal(t, "my_cluster", (&BackupMetadata{Tags: "embedded,cluster=my_cluster"}).EmbeddedCluster())
	assert.Equal(t, "c1", (&BackupMetadata{Tags: "cluster=c1,embedded"}).EmbeddedCluster())
}
