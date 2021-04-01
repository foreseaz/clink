package fibermsg

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/auxten/clink/core"
	"github.com/auxten/clink/ngncol"
	"github.com/auxten/clink/ngnrow"
)

type Rows struct {
	TableName string
	ColNames  []string
	Rows      [][]interface{}
}

func (r *Rows) String() string {
	return fmt.Sprintf("columns: %v\n@%d rows", r.ColNames, len(r.Rows))
}

func (r *Rows) ToDML(eng core.Engine) (sql string) {
	/*
		INSERT INTO table (column1, column2, ...) VALUES
			(value1, value2, ...),
			(value1, value2, ...);
	*/

	// values follow VALUES of INSERT
	values := make([]string, len(r.Rows))

	// Quote column names if possible
	colNames := make([]string, len(r.ColNames))

	// cols holds column data of every row, reused in every row iteration
	cols := make([]string, len(r.ColNames))

	switch eng.(type) {
	/*
		| Name        | Aliases                            | Description                                      |
		| :---------- | :--------------------------------- | :----------------------------------------------- |
		| `BIGINT`    | `INT8`, `LONG`                     | signed eight-byte integer                        |
		| `BOOLEAN`   | `BOOL`, `LOGICAL`                  | logical boolean (true/false)                     |
		| `BLOB`      | `BYTEA`, `BINARY,` `VARBINARY`     | variable-length binary data                      |
		| `DATE`      |                                    | calendar date (year, month day)                  |
		| `DOUBLE`    | `FLOAT8`, `NUMERIC`, `DECIMAL`     | double precision floating-point number (8 bytes) |
		| `HUGEINT`   |                                    | signed sixteen-byte integer                      |
		| `INTEGER`   | `INT4`, `INT`, `SIGNED`            | signed four-byte integer                         |
		| `REAL`      | `FLOAT4`, `FLOAT`                  | single precision floating-point number (4 bytes) |
		| `SMALLINT`  | `INT2`, `SHORT`                    | signed two-byte integer                          |
		| `TIMESTAMP` | `DATETIME`                         | time of day (no time zone)                       |
		| `TINYINT`   | `INT1`                             | signed one-byte integer                          |
		| `VARCHAR`   | `CHAR`, `BPCHAR`, `TEXT`, `STRING` | variable-length character string                 |
	*/
	case *ngncol.Engine:
		for i, colName := range r.ColNames {
			colNames[i] = colName
		}
		for i, row := range r.Rows {
			for j, col := range row {
				switch col.(type) {
				case int, int8, int16, int32, int64,
					uint, uint8, uint16, uint32, uint64:
					// Integer
					cols[j] = fmt.Sprintf("%d", col)
				case float32, float64:
					// Float
					cols[j] = fmt.Sprintf("%f", col)
				case []byte:
					// BLOB, Max 4GB
					cols[j] = `'\x` + hex.EncodeToString(col.([]byte)) + `'::BLOB`
				case string:
					// String
					cols[j] = "'" + col.(string) + "'"
				case bool:
					// Bool
					if col.(bool) {
						cols[j] = "TRUE"
					} else {
						cols[j] = "FALSE"
					}
				case time.Time:
					// A timestamp specifies a combination of DATE (year, month, day) and
					// a TIME (hour, minute, second, millisecond). Timestamps can be created
					// using the TIMESTAMP keyword, where the data must be formatted
					// according to the ISO 8601 format (YYYY-MM-DD hh:mm:ss).
					cols[j] = "'" + col.(time.Time).Format("2006-01-02 15:04:05") + "'"
				default:
					// Other stuff
					cols[j] = fmt.Sprintf("%s", col)
				}
			}

			values[i] = "(" + strings.Join(cols, ",") + ")"
		}
		sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;",
			r.TableName,
			strings.Join(colNames, ","),
			strings.Join(values, ","),
		)

	case *ngnrow.Engine:
		placeholders := ""
		// Values will produced by DMLArgs()
		for i, colName := range r.ColNames {
			colNames[i] = "`" + colName + "`"
			if i+1 == len(r.ColNames) {
				placeholders += "?"
			} else {
				placeholders += "?, "
			}
		}

		sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			r.TableName,
			strings.Join(colNames, ","),
			placeholders,
		)

	}
	return sql
}

func (r *Rows) DMLArgs(eng core.Engine) [][]interface{} {
	switch eng.(type) {
	case *ngncol.Engine:
		return nil
	case *ngnrow.Engine:
		return r.Rows
	default:
		panic("not supported engine")
	}
}
