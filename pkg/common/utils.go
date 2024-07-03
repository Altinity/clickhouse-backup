package common

import (
	"net/url"
	"strings"
)

func TablePathEncode(str string) string {
	return strings.NewReplacer(
		"!", "%21", "@", "%40", "#", "%23", "$", "%24", "^", "%5E", "&", "%26", "*", "%2A",
		"(", "%28", ")", "%29", "+", "%2B", "-", "%2D", "=", "%3D", "[", "%5B", "]", "%5D",
		"{", "%7B", "}", "%7D", "|", "%7C", ";", "%3B", "'", "%27", ":", "%3A", "\"", "%22",
		",", "%2C", ".", "%2E", "/", "%2F", "<", "%3C", ">", "%3E", "?", "%3F", "~", "%7E",
	).Replace(url.PathEscape(str))
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
