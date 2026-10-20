package server

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/1010
func TestResumableRestoreMappingArgs(t *testing.T) {
	r := require.New(t)
	// params come back from the bolt state as JSON, so []string turns into []interface{}
	var params map[string]interface{}
	r.NoError(json.Unmarshal([]byte(`{"databaseMapping":["db1:db1_copy","db2:db2_copy"],"tableMapping":["t1:t1_copy"],"tablePattern":"*"}`), &params))
	r.Equal([]string{
		"--restore-database-mapping=db1:db1_copy",
		"--restore-database-mapping=db2:db2_copy",
		"--restore-table-mapping=t1:t1_copy",
	}, resumableRestoreMappingArgs(params))

	// states written before the mappings were stored and states without mappings
	r.Empty(resumableRestoreMappingArgs(map[string]interface{}{"tablePattern": "*"}))
	r.Empty(resumableRestoreMappingArgs(map[string]interface{}{"databaseMapping": []interface{}{}}))
	r.Empty(resumableRestoreMappingArgs(nil))
}
