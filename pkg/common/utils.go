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

func AddStringToSliceIfNotExists(slice []string, newItem string) []string {
	exists := false
	for _, item := range slice {
		if item == newItem {
			exists = true
			break
		}
	}
	if !exists {
		slice = append(slice, newItem)
	}
	return slice
}

func AddSliceToSliceIfNotExists(existsSlice []string, newSlice []string) []string {
	for _, newItem := range newSlice {
		existsSlice = AddStringToSliceIfNotExists(existsSlice, newItem)
	}
	return existsSlice
}
