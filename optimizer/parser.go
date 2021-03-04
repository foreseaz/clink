package optimizer

import (
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
)

func sql2ast(sql string) (parser.Statements, error) {
	return parser.Parse(sql)
}
