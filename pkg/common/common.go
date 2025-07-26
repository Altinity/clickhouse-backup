package common

import (
	"math/rand"
	"net/url"
	"reflect"
	"strings"
	"time"
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

func CompareMaps(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	for key, valueA := range a {
		valueB, exists := b[key]
		if !exists || !deepEqual(valueA, valueB) {
			return false
		}
	}
	return true
}

func deepEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}

	ta := reflect.TypeOf(a)
	tb := reflect.TypeOf(b)

	// Приводим типы срезов к общему виду для корректного сравнения
	if (ta.Kind() == reflect.Slice || ta.Kind() == reflect.Array) &&
		(tb.Kind() == reflect.Slice || tb.Kind() == reflect.Array) {
		va := reflect.ValueOf(a)
		vb := reflect.ValueOf(b)

		if va.Len() != vb.Len() {
			return false
		}

		for i := 0; i < va.Len(); i++ {
			if !deepEqual(va.Index(i).Interface(), vb.Index(i).Interface()) {
				return false
			}
		}
		return true
	}

	// Для карт выполняем рекурсивное сравнение
	if ta.Kind() == reflect.Map && tb.Kind() == reflect.Map {
		va := reflect.ValueOf(a)
		vb := reflect.ValueOf(b)

		if va.Len() != vb.Len() {
			return false
		}

		iter := va.MapRange()
		for iter.Next() {
			key := iter.Key()
			valueA := iter.Value()
			valueB := vb.MapIndex(key)
			if !valueB.IsValid() || !deepEqual(valueA.Interface(), valueB.Interface()) {
				return false
			}
		}
		return true
	}

	// В остальных случаях используем reflect.DeepEqual
	return reflect.DeepEqual(a, b)
}

func AddRandomJitter(duration time.Duration, jitterPercent int8) time.Duration {
	if jitterPercent <= 0 {
		return duration
	}
	maxJitter := duration * time.Duration(jitterPercent) / 100
	jitter := time.Duration(rand.Int63n(int64(maxJitter + 1)))
	return duration + jitter
}

// CalculateChecksum calculates checksum for a file on a given disk
func CalculateChecksum(disk interface{ GetPath() string }, relativePath string) (uint64, error) {
	fullPath := path.Join(disk.GetPath(), relativePath)
	file, err := os.Open(fullPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	
	hash := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := io.Copy(hash, file); err != nil {
		return 0, err
	}
	
	return hash.Sum64(), nil
}
