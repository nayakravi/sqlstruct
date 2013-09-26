// Copyright 2012 Kamil Kisiel. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package sqlstruct

import (
	"database/sql"
	"reflect"
	"testing"
)

type testType struct {
	FieldA string `sql:"field_a"`
	FieldB string `sql:"-"`       // Ignored
	FieldC string `sql:"field_C"` // Different letter case
	FieldD string // Field name is used after converting to snake case
	FieldE bool
	FieldF int
}

// testRows is a mock version of sql.Rows which can only scan strings
type testRows struct {
	columns []string
	values  []interface{}
}

func (r testRows) Scan(dest ...interface{}) error {
	for i := range r.values {
		v := reflect.ValueOf(dest[i])
		if v.Kind() != reflect.Ptr {
			panic("Not a pointer!")
		}

		switch dest[i].(type) {
		case *string:
			*(dest[i].(*string)) = r.values[i].(string)
		case *sql.NullBool:
			(*dest[i].(*sql.NullBool)).Bool = r.values[i].(bool)
			(*dest[i].(*sql.NullBool)).Valid = true
		case *sql.NullInt64:
			dest[i] = nil
			(*dest[i].(*sql.NullInt64)).Valid = false
		case *sql.NullFloat64:
			(*dest[i].(*sql.NullFloat64)).Float64 = r.values[i].(float64)
			(*dest[i].(*sql.NullFloat64)).Valid = true
		case *sql.NullString:
			(*dest[i].(*sql.NullString)).String = r.values[i].(string)
			(*dest[i].(*sql.NullString)).Valid = true
		default:
			// Do nothing. We assume the tests only use strings here
		}
	}
	return nil
}

func (r testRows) Columns() ([]string, error) {
	return r.columns, nil
}

func (r *testRows) addValue(c string, v interface{}) {
	r.columns = append(r.columns, c)
	r.values = append(r.values, v)
}

func TestColumns(t *testing.T) {
	var v testType
	e := "field_a, field_c, field_d, field_e, field_f"
	c := Columns(v)

	if c != e {
		t.Errorf("expected %q got %q", e, c)
	}
}

func TestScan(t *testing.T) {
	rows := testRows{}
	rows.addValue("field_a", "a")
	rows.addValue("field_b", "b")
	rows.addValue("field_c", "c")
	rows.addValue("field_d", "d")
	rows.addValue("field_e", true)

	e := testType{"a", "", "c", "d", true, 0}

	var r testType
	err := Scan(&r, rows)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if r != e {
		t.Errorf("expected %q got %q", e, r)
	}
}
