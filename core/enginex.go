package core

type Engine interface {
	GetDDL(*Table) []string
	InitTables() (err error)
	ShowSchema() (s string, err error)
	ShowIndex() (s string, err error)
	Exec(msg FiberMsg) (err error)
	Query(query string, args ...interface{}) (columns []string, result [][]interface{}, err error)
}
