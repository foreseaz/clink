//+build darwin

package ngncol

import (
	"database/sql"

	"github.com/auxten/clink/core"
)

type Engine struct {
	Name   string
	Type   string
	Store  string
	db     *sql.DB
	Schema *core.Schema
}

func (e *Engine) GetDDL(t *core.Table) (ddl []string) {
	panic("ngncol not supported under darwin")
}

func (e *Engine) InitTables() (err error) {
	panic("ngncol not supported under darwin")
}

func (e *Engine) ShowSchema() (s string, err error) {
	panic("ngncol not supported under darwin")
}

func (e *Engine) ShowIndex() (s string, err error) {
	panic("ngncol not supported under darwin")
}

func (e *Engine) Exec(msg core.FiberMsg) (err error) {
	panic("ngncol not supported under darwin")
}

func (e *Engine) Query(query string, args ...interface{}) (columns []string, result [][]interface{}, err error) {
	panic("ngncol not supported under darwin")
}
