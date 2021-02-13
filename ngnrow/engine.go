package ngnrow

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"

	"github.com/auxten/clink/ngnrow/schema"
	"github.com/auxten/clink/utils"
)

type Engine struct {
	Name   string
	Type   string
	Store  string
	db     *sql.DB
	Schema *schema.Schema
}

func (e *Engine) InitTables() (err error) {
	if e.db, err = sql.Open(e.Type, e.Store); err != nil {
		log.Errorf("open memory engine failed %v", err)
		return
	}

	err = ExecuteTx(context.Background(), e.db, nil, func(tx *sql.Tx) error {
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
		if rows, err = e.db.Query(fmt.Sprintf(`SELECT sql
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

func (e *Engine) Exec(tbl string, query string, args ...interface{}) (err error) {
	msg := &schema.Msg{
		Value: []byte(query),
		Table: e.Schema.TableMap[tbl],
	}
	if _, err = e.db.Exec(msg.ToSQL(), args...); err != nil {
		log.WithError(err).Errorf("process msg %s", msg)
		return
	}
	return
}

func (e *Engine) Query(query string, args ...interface{}) (result [][]interface{}, err error) {
	var (
		rows *sql.Rows
	)

	log.Debugf("Query: %v, Args: %v", query, args)

	if rows, err = e.db.Query(query, args...); err != nil {
		log.WithError(err).Errorf("query %s with Args %v", query, args)
		return
	}
	defer rows.Close()
	if result, err = utils.ReadAllRows(rows); err != nil {
		log.WithError(err).Errorf("marshal rows to json")
		return
	}

	return
}