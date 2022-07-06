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

func getDisksByPath(disks []Disk, dataPath string) []string {
	resultDisks := []Disk{}
	for _, disk := range disks {
		if strings.HasPrefix(dataPath, disk.Path) {
			if len(resultDisks) == 0 {
				resultDisks = append(resultDisks, disk)
			} else {
				if len(disk.Path) > len(resultDisks[len(resultDisks)-1].Path) {
					resultDisks[len(resultDisks)-1] = disk
				} else if disk.Name != resultDisks[len(resultDisks)-1].Name && len(disk.Path) == len(resultDisks[len(resultDisks)-1].Path) {
					resultDisks = append(resultDisks, disk)
				}
			}
		}
	}
	if len(resultDisks) == 0 {
		return []string{"default"}
	} else {
		result := make([]string, len(resultDisks))
		for i, disk := range resultDisks {
			result[i] = disk.Name
		}
		return result
	}
}

func GetDisksByPaths(disks []Disk, dataPaths []string) map[string]string {
	result := map[string]string{}
	for _, dataPath := range dataPaths {
		for _, disk := range getDisksByPath(disks, dataPath) {
			result[disk] = dataPath
		}
	}
	return result
}
