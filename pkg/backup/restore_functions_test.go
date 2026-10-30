package backup

import (
	"testing"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/stretchr/testify/require"
)

// https://github.com/Altinity/clickhouse-backup/issues/1636
func TestPlanFunctionsRestore(t *testing.T) {
	backupFunctions := []metadata.FunctionsMeta{
		{Name: "f_missing", CreateQuery: "CREATE OR REPLACE FUNCTION f_missing AS (x) -> (x + 1)"},
		{Name: "f_same", CreateQuery: "CREATE OR REPLACE FUNCTION f_same AS (x) -> (x + 2)"},
		{Name: "f_changed", CreateQuery: "CREATE OR REPLACE FUNCTION f_changed AS (x) -> (x + 3)"},
		// backup created by hand without OR REPLACE
		{Name: "f_plain", CreateQuery: "CREATE FUNCTION f_plain AS (x) -> (x + 4)"},
	}
	existingFunctions := []clickhouse.Function{
		{Name: "f_same", CreateQuery: "CREATE OR REPLACE FUNCTION f_same AS (x) -> (x + 2)"},
		{Name: "f_changed", CreateQuery: "CREATE OR REPLACE FUNCTION f_changed AS (x) -> (x * 3)"},
		{Name: "f_plain", CreateQuery: "CREATE OR REPLACE FUNCTION f_plain AS (x) -> (x + 4)"},
		{Name: "f_not_in_backup", CreateQuery: "CREATE OR REPLACE FUNCTION f_not_in_backup AS (x) -> x"},
	}
	actions := func(plan []functionRestoreAction) map[string]string {
		result := map[string]string{}
		for _, p := range plan {
			result[p.Function.Name] = p.Action
		}
		return result
	}
	require.Equal(t, map[string]string{
		"f_missing": functionRestoreCreate,
		"f_same":    functionRestoreSkipIdentical,
		"f_changed": functionRestoreKeepExisting,
		"f_plain":   functionRestoreSkipIdentical,
	}, actions(planFunctionsRestore(backupFunctions, existingFunctions, false)))
	require.Equal(t, map[string]string{
		"f_missing": functionRestoreCreate,
		"f_same":    functionRestoreSkipIdentical,
		"f_changed": functionRestoreReplace,
		"f_plain":   functionRestoreSkipIdentical,
	}, actions(planFunctionsRestore(backupFunctions, existingFunctions, true)))
	require.Empty(t, planFunctionsRestore(nil, existingFunctions, true))
}
