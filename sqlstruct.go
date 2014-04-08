// Copyright 2012 Kamil Kisiel. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package sqlstruct provides some convenience functions for using structs with
the Go standard library's database/sql package.

The package matches struct field names to SQL query column names. Field names are
automatically converted to snake case. A field can also specify a matching column
with "sql" tag, if it's different from field name.  Unexported fields or fields marked
with `sql:"-"` are ignored, just like with "encoding/json" package. Anonymous fields
are currently ignored as well.

For example:

	type T struct {
		F1 string
		F2 string `sql:"field2"`
		F3 string `sql:"-"`
	}

	rows, err := db.Query(fmt.Sprintf("SELECT %s FROM tablename", sqlstruct.Columns(T)))
	...

	for rows.Next() {
		var t T
		err = sqlstruct.Scan(&t, rows)
		...
	}

	err = rows.Err() // get any errors encountered during iteration


*/
package sqlstruct

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// A cache of fieldInfos to save reflecting every time. Inspried by encoding/xml
var finfos map[reflect.Type]fieldInfo
var finfoLock sync.RWMutex

// tagName is the name of the tag to use on struct fields
const tagName = "sql"

// fieldInfo is a mapping of field tag values to their indices
type fieldInfo map[string]int

func init() {
	finfos = make(map[reflect.Type]fieldInfo)
}

// Rows defines the interface of types that are scannable with the Scan function.
// It is implemented by the sql.Rows type from the standard library
type Rows interface {
	Scan(...interface{}) error
	Columns() ([]string, error)
}

type nullableField struct {
	field reflect.Value
	value interface{}
}

func snakeCasedName(name string) string {
	newstr := make([]rune, 0)
	firstTime := true

	for _, chr := range name {
		if isUpper := 'A' <= chr && chr <= 'Z'; isUpper {
			if firstTime == true {
				firstTime = false
			} else {
				newstr = append(newstr, '_')
			}
			chr -= ('A' - 'a')
		}
		newstr = append(newstr, chr)
	}

	return string(newstr)
}

// getFieldInfo creates a fieldInfo for the provided type. Fields that are not tagged
// with the "sql" tag and unexported fields are not included.
func getFieldInfo(typ reflect.Type) fieldInfo {
	finfoLock.RLock()
	finfo, ok := finfos[typ]
	finfoLock.RUnlock()
	if ok {
		return finfo
	}

	finfo = make(fieldInfo)

	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		tag := f.Tag.Get(tagName)

		// Skip unexported fields, fields marked with "-" or anonymous fields
		if f.PkgPath != "" || tag == "-" || f.Anonymous {
			continue
		}

		// Use field name for untagged fields
		if tag == "" {
			tag = snakeCasedName(f.Name)
		} else {
			tag = strings.ToLower(tag)
		}

		finfo[tag] = i
	}

	finfoLock.Lock()
	finfos[typ] = finfo
	finfoLock.Unlock()

	return finfo
}

// Scan scans the next row from rows in to a struct pointed to by dest. The struct type
// should have exported fields tagged with the "sql" tag. Columns from row which are not
// mapped to any struct fields are ignored. Struct fields which have no matching column
// in the result set are left unchanged.
func Scan(dest interface{}, rows Rows) error {
	destv := reflect.ValueOf(dest)
	typ := destv.Type()

	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Errorf("dest must be pointer to struct; got %T", destv))
	}
	fieldInfo := getFieldInfo(typ.Elem())

	elem := destv.Elem()
	var values []interface{}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	var nullableFields []nullableField

	for _, name := range cols {
		idx, ok := fieldInfo[strings.ToLower(name)]
		var v interface{}
		if !ok {
			// There is no field mapped to this column so we discard it
			v = &sql.RawBytes{}
		} else {
			//Substitute nullable fields to transparently support them
			switch elem.Field(idx).Kind() {
			case reflect.Bool:
				v = &sql.NullBool{}
				nullableFields = append(nullableFields, nullableField{field: elem.Field(idx), value: v})
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v = &sql.NullInt64{}
				nullableFields = append(nullableFields, nullableField{field: elem.Field(idx), value: v})
			case reflect.Float32, reflect.Float64:
				v = &sql.NullFloat64{}
				nullableFields = append(nullableFields, nullableField{field: elem.Field(idx), value: v})
			case reflect.String:
				v = &sql.NullString{}
				nullableFields = append(nullableFields, nullableField{field: elem.Field(idx), value: v})
			default:
				v = elem.Field(idx).Addr().Interface()
			}
		}
		values = append(values, v)
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	for _, nullableField := range nullableFields {
		switch nullableField.value.(type) {
		case *sql.NullBool:
			value := *nullableField.value.(*sql.NullBool)
			if value.Valid {
				nullableField.field.SetBool(value.Bool)
			} else {
				nullableField.field.SetBool(false)
			}
		case *sql.NullInt64:
			value := *nullableField.value.(*sql.NullInt64)
			if value.Valid {
				nullableField.field.SetInt(value.Int64)
			} else {
				nullableField.field.SetInt(0)
			}
		case *sql.NullFloat64:
			value := *nullableField.value.(*sql.NullFloat64)
			if value.Valid {
				nullableField.field.SetFloat(value.Float64)
			} else {
				nullableField.field.SetFloat(0)
			}
		case *sql.NullString:
			value := *nullableField.value.(*sql.NullString)
			if value.Valid {
				nullableField.field.SetString(value.String)
			} else {
				nullableField.field.SetString("")
			}
		}
	}

	return nil
}

// Columns returns a string containing a sorted, comma-separated list of column names as defined
// by the type s. s must be a struct that has exported fields tagged with the "sql" tag.
func Columns(s interface{}) string {
	v := reflect.ValueOf(s)
	fields := getFieldInfo(v.Type())

	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}

	sort.Strings(names)
	return strings.Join(names, ", ")
}

func NullValue(value interface{}) interface{} {
	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v.Int() == 0 {
			return sql.NullInt64{Int64: 0, Valid: false}
		} else {
			return value
		}
	case reflect.Float32, reflect.Float64:
		if v.Float() == 0 {
			return sql.NullFloat64{Float64: 0, Valid: false}
		} else {
			return value
		}
	case reflect.String:
		if strings.TrimSpace(value.(string)) == "" {
			return sql.NullString{String: "", Valid: false}
		} else {
			return value
		}
	}

	return value
}
