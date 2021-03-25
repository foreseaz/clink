package fibermsg

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"github.com/auxten/clink/core"
	"github.com/auxten/clink/ngncol"
	"github.com/auxten/clink/ngnrow"
)

/*
	INSERT:
		{
		    "after": {
		        "TANS_AMT": "100.01",
		        "TRANS_FLAG": "P",
		        "TRANS_DATE": "2001-03-08 23:21:00",
		        "TRANS_BRAN_CODE": "11670103",
		        "MC_TRSCODE": "CWD"
		    },
		    "rowid": "623481",
		    "scntime": 984064860,
		    "optype": "INSERT",
		    "name": "MJ_JOUR"
		}
	UPDATE:
		{
			"rowid": "623481",
			"scntime": 984064861,
			"optype": "UPDATE",
			"name": "MJ_JOUR",
			"after": {
				"TRANS_FLAG": "0"
			},
			"before": {
				"TRANS_FLAG": "p"
			}
		}
*/

type JsonMsg struct {
	Value       []byte
	Table       *core.Table
	ArgsA       []interface{}
	DMLTypePath string
}

func isNumeric(s string) bool {
	return strings.Contains(strings.ToLower(s), "int")
}

func (m *JsonMsg) String() string {
	return fmt.Sprintf("%s on %s", string(m.Value), m.Table.Name)
}

func (m *JsonMsg) Args() []interface{} {
	return m.ArgsA
}

func (m *JsonMsg) ToDML(eng core.Engine) string {
	var (
		sql    string
		cols   []string
		values []string
	)
	msg := gjson.ParseBytes(m.Value)
	sqlType := msg.Get(m.DMLTypePath)
	lowerCaseType := strings.ToLower(sqlType.Str)
	switch lowerCaseType {
	case "insert":
		/*
			INSERT INTO table (column1, column2, ...)
				VALUES(value1, value2, ...);
		*/
		cols = make([]string, 0, len(m.Table.Cols))
		values = make([]string, 0, len(m.Table.Cols))
		for _, col := range m.Table.Cols {
			if insVal := msg.Get(col.InsertPath); insVal.Exists() {
				switch eng.(type) {
				case *ngncol.Engine:
					cols = append(cols, fmt.Sprintf("%s", col.Name))
				case *ngnrow.Engine:
					cols = append(cols, fmt.Sprintf("`%s`", col.Name))
				}
				if isNumeric(col.Type) {
					values = append(values, insVal.String())
				} else {
					values = append(values, fmt.Sprintf(`'%s'`, insVal.String()))
				}
			}
		}

		sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			m.Table.Name,
			strings.Join(cols, ","),
			strings.Join(values, ","),
		)
	case "update":
		/*
			UPDATE table
			SET column_1 = new_value_1,
				column_2 = new_value_2
			WHERE
				rowid = xxxx
			LIMIT 1
		*/
		var value string
		sets := make([]string, 0, len(m.Table.Cols))
		where := make([]string, 0, 1)
		for _, col := range m.Table.Cols {
			if updateVal := msg.Get(col.UpdatePath); updateVal.Exists() {
				if isNumeric(col.Type) {
					value = updateVal.String()
				} else {
					value = fmt.Sprintf(`'%s'`, updateVal.String())
				}
				switch eng.(type) {
				case *ngncol.Engine:
					if col.Name == m.Table.Pk {
						where = append(where, fmt.Sprintf(`%s = %s`, col.Name, value))
					} else {
						sets = append(sets, fmt.Sprintf(`%s = %s`, col.Name, value))
					}
				case *ngnrow.Engine:
					if col.Name == m.Table.Pk {
						where = append(where, fmt.Sprintf("`%s` = %s", col.Name, value))
					} else {
						sets = append(sets, fmt.Sprintf("`%s` = %s", col.Name, value))
					}
				}
			}
		}
		switch eng.(type) {
		case *ngncol.Engine:
			sql = fmt.Sprintf(`UPDATE %s SET %s WHERE %s;`,
				m.Table.Name,
				strings.Join(sets, ", "),
				strings.Join(where, " AND "),
			)
		case *ngnrow.Engine:
			sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s;",
				m.Table.Name,
				strings.Join(sets, ", "),
				strings.Join(where, " AND "),
			)
		}

	case "delete":
		/*
			DELETE FROM table WHERE search_condition LIMIT 1;
		*/
		if pk := msg.Get(m.Table.Pk); pk.Exists() {
			switch eng.(type) {
			case *ngncol.Engine:
				sql = fmt.Sprintf(`DELETE FROM %s WHERE %s = '%s' LIMIT 1;`, m.Table.Name, m.Table.Pk, pk.String())
			case *ngnrow.Engine:
				sql = fmt.Sprintf("DELETE FROM %s WHERE %s = %s LIMIT 1;", m.Table.Name, m.Table.Pk, pk.String())
			}
		}

	default:
		log.Debugf("not supported SQL type %s", sqlType.Str)
	}

	return sql
}
