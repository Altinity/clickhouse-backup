package common

import (
	"net/url"
	"strings"
)

func TablePathEncode(str string) string {
	return strings.NewReplacer(".", "%2E", "-", "%2D").Replace(url.PathEscape(str))

}

func SumMapValuesInt(m map[string]int) int {
	s := 0
	for _, v := range m {
		s += v
	}
	return s
}
