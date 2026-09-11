package keeper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// expectations taken from ClickHouse src/Common/escapeForFileName.cpp
func TestEscapeForFileName(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty", input: "", expected: ""},
		{name: "alphanumeric and underscore kept as is", input: "test_rbac0AZ", expected: "test_rbac0AZ"},
		{name: "dot and dash", input: "test.rbac-name", expected: "test%2Erbac%2Dname"},
		{name: "space and backquote", input: "`a` ON b", expected: "%60a%60%20ON%20b"},
		{name: "row policy full name", input: "`test.rbac-name` ON test_rbac.test_rbac", expected: "%60test%2Erbac%2Dname%60%20ON%20test_rbac%2Etest_rbac"},
		{name: "utf8 escaped byte-wise", input: "имя", expected: "%D0%B8%D0%BC%D1%8F"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, EscapeForFileName(tc.input))
		})
	}
}
