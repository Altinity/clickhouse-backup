package clickhouse

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

func fieldsByTraversal(v reflect.Value, traversals [][]int, values []interface{}, ptrs bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(interface{})
			continue
		}
		f := reflectx.FieldByIndexes(v, traversal)
		if ptrs {
			values[i] = f.Addr().Interface()
		} else {
			values[i] = f.Interface()
		}
	}
	return nil
}

func getDiskByPath(disks []Disk, dataPath string) string {
	resultDisk := Disk{}
	for _, disk := range disks {
		if strings.HasPrefix(dataPath, disk.Path) && len(disk.Path) > len(resultDisk.Path) {
			resultDisk = disk
		}
	}
	if resultDisk.Name == "" {
		return "unknown"
	} else {
		return resultDisk.Name
	}
}

func GetDisksByPaths(disks []Disk, dataPaths []string) map[string]string {
	result := map[string]string{}
	for _, dataPath := range dataPaths {
		result[getDiskByPath(disks, dataPath)] = dataPath
	}
	return result
}
