package ngncol

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/auxten/clink-core" // clink column store
	log "github.com/sirupsen/logrus"

	"github.com/auxten/clink/ngncol/schema"
)

type Engine struct {
	name   string
	Db     *sql.DB
	Schema *schema.Schema
}

func NewEngine(name string, s *schema.Schema) *Engine {
	return &Engine{
		name:   name,
		Schema: s,
	}
}

func (e *Engine) InitTables() (err error) {
	if e.Db, err = sql.Open("clink", "atmj.db"); err != nil {
		log.Errorf("open memory engine failed %v", err)
		return
	}

	err = ExecuteTx(context.Background(), e.Db, nil, func(tx *sql.Tx) error {
		for _, table := range e.Schema.Tables {
			for _, ddl := range table.DDL() {
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
		if rows, err = e.Db.Query(fmt.Sprintf(`SELECT sql
FROM sqlite_master
WHERE type = 'table' AND tbl_name = '%s'
	AND tbl_name NOT LIKE 'sqlite%%'`, table.Name)); err != nil {
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
		if rows, err = e.Db.Query(fmt.Sprintf(`SELECT sql
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

func (e *Engine) Exec(tbl string, m []byte) (err error) {
	msg := &schema.Msg{
		Value: m,
		Table: e.Schema.TableMap[tbl],
	}
	if _, err = e.Db.Exec(msg.ToSQL()); err != nil {
		log.WithError(err).Errorf("process msg %s", msg)
		return
	}
	return
}
