package clickhouse

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

func GetDiskByPath(disks []Disk, dataPath string) string {
	prefixLengthMap := map[int]string{}

	for _, disk := range disks {
		if strings.HasPrefix(dataPath, disk.Path) {
			prefixLengthMap[len(disk.Path)] = disk.Name
		}
	}

	if len(prefixLengthMap) == 0 {
		return "unknown"
	}

	var maxPrefixLength int
	for prefixLength := range prefixLengthMap {
		if prefixLength > maxPrefixLength {
			maxPrefixLength = prefixLength
		}
	}

	return prefixLengthMap[maxPrefixLength]
}

func GetDisksByPaths(disks []Disk, dataPaths []string) map[string]string {
	result := map[string]string{}
	for _, dataPath := range dataPaths {
		result[GetDiskByPath(disks, dataPath)] = dataPath
	}
	return result
}

func (ch *ClickHouse) softSelect(dest interface{}, query string) error {
	rows, err := ch.Queryx(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var v, vp reflect.Value

	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return fmt.Errorf("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return err
	}

	isPtr := slice.Elem().Kind() == reflect.Ptr
	base := reflectx.Deref(slice.Elem())

	columns, err := rows.Columns()
	fields := rows.Mapper.TraversalsByName(base, columns)
	values := make([]interface{}, len(columns))

	if err != nil {
		return err
	}
	for rows.Next() {
		vp = reflect.New(base)
		v = reflect.Indirect(vp)

		err = fieldsByTraversal(v, fields, values, true)
		if err != nil {
			return err
		}

		// scan into the struct field pointers and append to our results
		err = rows.Scan(values...)
		if err != nil {
			return err
		}

		if isPtr {
			direct.Set(reflect.Append(direct, vp))
		} else {
			direct.Set(reflect.Append(direct, v))
		}

		// rt := reflect.TypeOf(s)
		// if rt.Kind() != reflect.Struct {
		// 	return fmt.Errorf("bad type")
		// }
		// for i := 0; i < rt.NumField(); i++ {
		// 	f := rt.Field(i)
		// 	v := strings.Split(f.Tag.Get(key), ",")[0] // use split to ignore tag "options"
		// 	if v == tag {
		// 		return f.Name
		// 	}
		// }
		// return ""
	}
	return rows.Err()
}

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
