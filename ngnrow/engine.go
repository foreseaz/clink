package ngnrow

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"

	"github.com/auxten/clink/core"
	"github.com/auxten/clink/utils"
)

type Engine struct {
	Name   string
	Type   string
	Store  string
	db     *sql.DB
	Schema *core.Schema
}

func (e *Engine) GetDDL(t *core.Table) (ddl []string) {
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
			cols += fmt.Sprintf(`'%s' %s PRIMARY KEY`, col.Name, col.Type)
		} else {
			cols += fmt.Sprintf(`'%s' %s`, col.Name, col.Type)
		}
		if col.Extra != "" {
			cols += fmt.Sprintf(" %s", col.Extra)
		}
		if i != len(t.Cols)-1 {
			cols += ",\n"
		}
	}
	ddl[0] = fmt.Sprintf("CREATE TABLE IF NOT EXISTS '%s' (\n%s\n);", t.Name, cols)

	for _, idx := range t.Index {
		ddl = append(ddl,
			fmt.Sprintf(
				"CREATE INDEX IF NOT EXISTS 'idx__%s__%s' ON `%s` (`%s`);",
				t.Name, idx, t.Name, idx,
			))
	}
	return
}

func (e *Engine) InitTables() (err error) {
	if e.db, err = sql.Open(e.Type, e.Store); err != nil {
		log.Errorf("open memory engine failed %v", err)
		return
	}
	// for sqlite memory engine, set max open connections to 1 to avoid database locking error
	e.db.SetMaxOpenConns(1)
	err = utils.ExecuteTx(context.Background(), e.db, nil, func(tx *sql.Tx) error {
		for _, table := range e.Schema.Tables {
			ddls := e.GetDDL(&table)
			for _, ddl := range ddls {
				_, er := tx.Exec(ddl)
				if er != nil {
					log.WithError(er).Errorf("exec sql failed with ddl: %v", ddl)
					return er
				}
			}
		}
		return nil
	})
	if err != nil {
		log.WithError(err).Error("init tables")
		return
	}
	return
}

func (e *Engine) ShowSchema() (s string, err error) {
	for _, table := range e.Schema.Tables {
		var (
			rows *sql.Rows
		)
		if rows, err = e.db.Query(
			fmt.Sprintf(
				`SELECT sql FROM sqlite_master
WHERE type = 'table' AND tbl_name = '%s' AND tbl_name NOT LIKE 'sqlite%%'`, table.Name),
		); err != nil {
			log.WithError(err).Error("get all schema failed")
			return
		}

		for rows.Next() {
			var line string
			if err = rows.Scan(&line); err != nil {
				log.WithError(err).Error("scan rows of sqlite_master")
				_ = rows.Close()
				return
			}
			s += line
		}
		_ = rows.Close()
	}
	return
}

func (e *Engine) ShowIndex() (s string, err error) {
	for _, table := range e.Schema.Tables {
		var (
			rows *sql.Rows
		)
		if rows, err = e.db.Query(fmt.Sprintf(`SELECT sql
FROM sqlite_master
WHERE type = 'index' AND tbl_name = '%s'
	AND name NOT LIKE 'sqlite%%'`, table.Name)); err != nil {
			log.WithError(err).Error("get all index failed")
			return
		}
		for rows.Next() {
			var line string
			if err = rows.Scan(&line); err != nil {
				log.WithError(err).Error("scan rows of sqlite_master")
				_ = rows.Close()
				return
			}
			s += line + ";\n"
		}
		_ = rows.Close()
	}
	return
}

func (e *Engine) Exec(msg core.FiberMsg) (err error) {
	var (
		st   *sql.Stmt
		args [][]interface{}
	)
	args = msg.DMLArgs(e)
	if len(args) == 0 {
		if _, err = e.db.Exec(msg.ToDML(e)); err != nil {
			return
		}
	} else {
		st, err = e.db.Prepare(msg.ToDML(e))
		if err != nil {
			return err
		}
		defer st.Close()
		for _, arg := range args {
			if _, err = st.Exec(arg...); err != nil {
				return err
			}
		}
	}

	return
}

func (e *Engine) Query(query string, args ...interface{}) (columns []string, result [][]interface{}, err error) {
	var (
		rows *sql.Rows
	)

	log.Debugf("Query: %v, Args: %v", query, args)

	if rows, err = e.db.Query(query, args...); err != nil {
		log.WithError(err).Errorf("query %s with Args %v", query, args)
		return
	}
	defer rows.Close()
	if columns, result, err = utils.ReadAllRowsPtr(rows); err != nil {
		log.WithError(err).Errorf("marshal rows to json")
		return
	}

	return
}
