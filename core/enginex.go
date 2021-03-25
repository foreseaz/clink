package core

type Engine interface {
	InitTables() (err error)
	ShowSchema() (s string, err error)
	ShowIndex() (s string, err error)
	Exec(msg FiberMsg) (err error)
	Query(query string, args ...interface{}) (result [][]interface{}, err error)
}
