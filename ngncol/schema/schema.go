//+build linux

package schema

import (
	"fmt"

	"github.com/auxten/clink/core"
)

func GetDDL(t *core.Table) (ddl []string) {
	/*
		`CREATE TABLE IF NOT EXISTS "indexed_blocks" (
			"height"		INTEGER PRIMARY KEY,
			"hash"			TEXT NOT NULL,
			"timestamp"		INTEGER DEFAULT 0,
			"version"		INTEGER,
			"producer"		TEXT,
			"merkle_root"	TEXT,
			"parent"		TEXT,
			"tx_count"		INTEGER
		);`,
	*/
	ddl = make([]string, 1)

	cols := ""
	for i, col := range t.Cols {
		if col.Name == t.Pk {
			cols += fmt.Sprintf(`%s %s`, col.Name, col.Type)
		} else {
			cols += fmt.Sprintf(`%s %s`, col.Name, col.Type)
		}
		if col.Extra != "" {
			cols += fmt.Sprintf(" %s", col.Extra)
		}
		if i != len(t.Cols)-1 {
			cols += ",\n"
		}
	}
	ddl[0] = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);", t.Name, cols)

	for _, idx := range t.Index {
		ddl = append(ddl,
			fmt.Sprintf(
				`CREATE INDEX IF NOT EXISTS idx__%s__%s ON %s (%s);`,
				t.Name, idx, t.Name, idx,
			))
	}
	return
}
