//+build linux

package ngnx

import (
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/auxten/clink/core"
	"github.com/auxten/clink/ngncol"
	"github.com/auxten/clink/ngnrow"
)

// NewEngine create a new SQL engine.
// Args: typ show be "type+dsn" like string, eg.
//
//	| DB String        | Row/Col Based Engine | DB Store |
//	|------------------|----------------------|----------|
//	| compact+:memory: |          Row         | Memory   |
//	| compact+atmj.db  |          Row         | File     |
//	| row+atmj.db      |          Row         | File     |
//	| clink+:memory:   |          Col         | Memory   |
//	| clink+atmj.db    |          Col         | File     |
//	| column+:memory:  |          Col         | Memory   |
//	| col+:memory:     |          Col         | Memory   |
func NewEngine(typeString string, name string, schema *core.Schema) core.Engine {
	var (
		store string
	)

	dsn := strings.Split(typeString, "+")
	typ := dsn[0]
	if len(dsn) == 2 {
		store = dsn[1]
	}
	switch typ {
	case "compact", "row":
		eng := &ngnrow.Engine{
			Name:   name,
			Type:   "sqlite3",
			Store:  store,
			Schema: schema,
		}
		log.Debugf("Booting tubro %s with %s store", eng.Type, eng.Store)
		return eng

	case "clink", "col", "column":
		eng := &ngncol.Engine{
			Name:   name,
			Type:   "clink",
			Store:  store,
			Schema: schema,
		}
		log.Debugf("Booting tubro %s with %s store", eng.Type, eng.Store)
		return eng

	default:
		log.Fatalf("Unknown engine %s", typ)
	}

	return nil
}
