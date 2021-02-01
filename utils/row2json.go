package utils

import (
	"database/sql"
	"io"
	"time"
)

type rowScanner struct {
	fieldCnt int
	column   int           // current column
	fields   []interface{} // temp fields
	args     []interface{}
}

func newRowScanner(fieldCnt int) (s *rowScanner) {
	s = &rowScanner{
		fieldCnt: fieldCnt,
		column:   0,
		fields:   make([]interface{}, fieldCnt),
		args:     make([]interface{}, fieldCnt),
	}

	for i := 0; i != fieldCnt; i++ {
		s.args[i] = s
	}

	return
}

func (s *rowScanner) Scan(src interface{}) error {
	if s.fieldCnt <= s.column {
		// read complete
		return io.EOF
	}

	// type conversions
	switch srcValue := src.(type) {
	case []byte:
		s.fields[s.column] = string(srcValue)
	case bool:
		if srcValue {
			s.fields[s.column] = int8(1)
		} else {
			s.fields[s.column] = int8(0)
		}
	case time.Time:
		s.fields[s.column] = srcValue.String()
	default:
		s.fields[s.column] = src
	}

	s.column++

	return nil
}

func (s *rowScanner) getRow() []interface{} {
	return s.fields
}

func (s *rowScanner) scanArgs() []interface{} {
	// reset
	s.column = 0
	s.fields = make([]interface{}, s.fieldCnt)
	return s.args
}

func ReadAllRows(rows *sql.Rows) (result [][]interface{}, err error) {
	var columns []string
	if columns, err = rows.Columns(); err != nil {
		return
	}

	rs := newRowScanner(len(columns))
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
