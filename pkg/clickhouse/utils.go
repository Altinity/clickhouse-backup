package clickhouse

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx/reflectx"
)

func GetDiskByPath(disks []Disk, dataPath string) string {
	for _, disk := range disks {
		if strings.HasPrefix(dataPath, disk.Path) {
			return disk.Name
		}
	}
	return "unknown"
}

func GetDisksByPaths(disks []Disk, dataPaths []string) map[string]string {
	result := map[string]string{}
	for _, dataPath := range dataPaths {
		result[GetDiskByPath(disks, dataPath)] = dataPath
	}
	return result
}

// func (ch *ClickHouse) softSelect2(dest interface{}, query string) error {
// 	rows, err := ch.conn.Queryx(query)
// 	if err != nil {
// 		return err
// 	}
// 	defer rows.Close()
// 	for rows.Next() {
//         s := reflect.ValueOf(&dest).Elem()
//         numCols := s.NumField()
//         columns := make([]interface{}, numCols)
//         for i := 0; i < numCols; i++ {
//             field := s.Field(i)
//             columns[i] = field.Addr().Interface()
//         }

//         err := rows.Scan(columns...)
//         if err != nil {
//             return err
//         }
// 	}
// 	return nil
// }

func (ch *ClickHouse) softSelect(dest interface{}, query string) error {
	rows, err := ch.conn.Queryx(query)
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

// func (ch *ClickHouse) softSelect(dest interface{}, query string) error {
// 	var v, vp reflect.Value
// 	value := reflect.ValueOf(dest)

// 	// json.Unmarshal returns errors for these
// 	if value.Kind() != reflect.Ptr {
// 		return errors.New("must pass a pointer, not a value, to StructScan destination")
// 	}
// 	if value.IsNil() {
// 		return errors.New("nil pointer passed to StructScan destination")
// 	}
// 	direct := reflect.Indirect(value)

// 	slice, err := baseType(value.Type(), reflect.Slice)
// 	if err != nil {
// 		return err
// 	}

// 	isPtr := slice.Elem().Kind() == reflect.Ptr
// 	base := reflectx.Deref(slice.Elem())
// 	scannable := isScannable(base)

// 	if structOnly && scannable {
// 		return structOnlyError(base)
// 	}

// 	columns, err := rows.Columns()
// 	if err != nil {
// 		return err
// 	}

// 	// if it's a base type make sure it only has 1 column;  if not return an error
// 	if scannable && len(columns) > 1 {
// 		return fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(columns))
// 	}

// 	if !scannable {
// 		var values []interface{}
// 		var m *reflectx.Mapper

// 		switch rows.(type) {
// 		case *Rows:
// 			m = rows.(*Rows).Mapper
// 		default:
// 			m = mapper()
// 		}

// 		fields := m.TraversalsByName(base, columns)
// 		// if we are not unsafe and are missing fields, return an error
// 		if f, err := missingFields(fields); err != nil && !isUnsafe(rows) {
// 			return fmt.Errorf("missing destination name %s in %T", columns[f], dest)
// 		}
// 		values = make([]interface{}, len(columns))

// 		for rows.Next() {
// 			// create a new struct type (which returns PtrTo) and indirect it
// 			vp = reflect.New(base)
// 			v = reflect.Indirect(vp)

// 			err = fieldsByTraversal(v, fields, values, true)
// 			if err != nil {
// 				return err
// 			}

// 			// scan into the struct field pointers and append to our results
// 			err = rows.Scan(values...)
// 			if err != nil {
// 				return err
// 			}

// 			if isPtr {
// 				direct.Set(reflect.Append(direct, vp))
// 			} else {
// 				direct.Set(reflect.Append(direct, v))
// 			}
// 		}
// 	} else {
// 		for rows.Next() {
// 			vp = reflect.New(base)
// 			err = rows.Scan(vp.Interface())
// 			if err != nil {
// 				return err
// 			}
// 			// append
// 			if isPtr {
// 				direct.Set(reflect.Append(direct, vp))
// 			} else {
// 				direct.Set(reflect.Append(direct, reflect.Indirect(vp)))
// 			}
// 		}
// 	}

// 	return rows.Err()

// }
