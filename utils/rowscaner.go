package utils

import (
	"database/sql"
	"reflect"

	log "github.com/sirupsen/logrus"
)

type rowScanner struct {
	fieldCnt int
	column   int
	values   []interface{} // type normalized columns
	types    []reflect.Type
}

func newRowScanner(cts []*sql.ColumnType) (s *rowScanner) {
	s = &rowScanner{
		fieldCnt: len(cts),
	}

	s.types = make([]reflect.Type, len(cts))
	for i, tp := range cts {
		st := tp.ScanType()
		if st == nil {
			log.Debugf("scantype is null for column %q", tp.Name())
			continue
		}
		s.types[i] = st
	}

	return
}

func (s *rowScanner) getRow() []interface{} {
	return s.values
}

func (s *rowScanner) scanArgs() []interface{} {
	s.values = make([]interface{}, s.fieldCnt)
	for i, t := range s.types {
		if t == nil {
			s.values[i] = new(interface{})
		} else if t.Kind() == reflect.Slice {
			// varchar will be slice in result
			s.values[i] = new(string)
		} else {
			s.values[i] = reflect.New(t).Interface()
		}
	}
	return s.values
}

func ReadAllRowsPtr(rows *sql.Rows) (columns []string, result [][]interface{}, err error) {
	if columns, err = rows.Columns(); err != nil {
		return
	}

	tt, err := rows.ColumnTypes()
	if err != nil {
		return
	}

	rs := newRowScanner(tt)
	result = make([][]interface{}, 0)

	for rows.Next() {
		err = rows.Scan(rs.scanArgs()...)
		if err != nil {
			return
		}

		result = append(result, rs.getRow())
	}

	err = rows.Err()

	return
}

// ReadRowsIntoChanAsync reads rows and puts them into the ch channel in background
// close the rows will make the background job stopped
func ReadRowsIntoChanAsync(rows *sql.Rows, ch chan []interface{}) (columns []string, err error) {
	if columns, err = rows.Columns(); err != nil {
		return
	}

	go func(rows *sql.Rows, ch chan []interface{}) {
		defer close(ch)
		tt, err := rows.ColumnTypes()
		if err != nil {
			return
		}

		rs := newRowScanner(tt)

		for rows.Next() {
			err = rows.Scan(rs.scanArgs()...)
			if err != nil {
				return
			}

			ch <- rs.getRow()
		}
	}(rows, ch)

	return
}
