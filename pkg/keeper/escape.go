package keeper

import (
	"strings"
)

const hexDigits = "0123456789ABCDEF"

// EscapeForFileName - Go port of ClickHouse src/Common/escapeForFileName.cpp,
// used to build the `<zookeeper_path>/<type char>/<name>` znode name for RBAC objects
// stored in a `replicated` user directory
func EscapeForFileName(s string) string {
	var res strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			res.WriteByte(c)
		} else {
			res.WriteByte('%')
			res.WriteByte(hexDigits[c/16])
			res.WriteByte(hexDigits[c%16])
		}
	}
	return res.String()
}
