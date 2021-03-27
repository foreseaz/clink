package mysql

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"

	"github.com/auxten/clink/core"
)

type Fiber struct {
	Table  core.Table
	Engine core.Engine
	db     *sql.DB
	Ctx    context.Context
}

func (f *Fiber) Attach(ctx context.Context) (err error) {
	f.Ctx = ctx
	if f.db, err = sql.Open("mysql", f.Table.MySQLSrc.Dsn); err != nil {
		log.Errorf("Attach db failed: %v", err)
		return
	}

	return
}

func (f *Fiber) InitTables() (err error) {
	return f.Engine.InitTables()
}

func (f *Fiber) Detach() {
	if f.db != nil {
		_ = f.db.Close()
	}
}

func (f *Fiber) Pull() {
	if f.Table.MySQLSrc.Table != "" {
		f.db.QueryContext(f.Ctx)
	} else if f.Table.MySQLSrc.Select != "" {
		f.db.QueryContext(f.Ctx, f.Table.MySQLSrc.Select)
	}
}

func (f *Fiber) ScheduledPull() {

}
