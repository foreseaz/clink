package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"

	"github.com/auxten/clink/core"
	"github.com/auxten/clink/fibermsg"
	"github.com/auxten/clink/utils"
)

type Fiber struct {
	Table *core.Table
	Eng   core.Engine
	db    *sql.DB
	ctx   context.Context
}

func (f *Fiber) Attach(ctx context.Context) (err error) {
	f.ctx = ctx
	if f.db, err = sql.Open("mysql", f.Table.MySQLSrc.Dsn); err != nil {
		log.Errorf("Attach db failed: %v", err)
		return
	}

	return
}

func (f *Fiber) InitTables() (err error) {
	return f.Eng.InitTables()
}

func (f *Fiber) Detach() {
	if f.db != nil {
		_ = f.db.Close()
	}
}

func (f *Fiber) Pull() (err error) {
	var (
		fetchRows *sql.Rows
		rowsMsg   *fibermsg.Rows
		colNames  []string
	)
	if f.Table.MySQLSrc.Table != "" {
		//f.db.QueryContext(f.ctx,)
	} else if f.Table.MySQLSrc.Select != "" {
		fetchRows, err = f.db.QueryContext(f.ctx, f.Table.MySQLSrc.Select)
		if err != nil {
			return
		}
		ch := make(chan []interface{}, 128)
		colNames, err = utils.ReadRowsIntoChanAsync(fetchRows, ch)
		if err != nil {
			return
		}
	loop:
		for {
			// max 32 rows per batch
			msgs := make([][]interface{}, 0, 32)

			for i := 0; i < 32; i++ {
				select {
				case <-f.ctx.Done():
					err = errors.New("pull job cancelled")
					break loop
				case msg, ok := <-ch:
					if ok {
						if i == 0 {
							jb, _ := json.Marshal(msg)
							log.Debugf("%s", string(jb))
						}
						msgs = append(msgs, msg)
					} else {
						break loop
					}
				}
			}
			rowsMsg = &fibermsg.Rows{
				TableName: f.Table.Name,
				ColNames:  colNames,
				Rows:      msgs,
			}
			if err = f.Eng.Exec(rowsMsg); err != nil {
				return
			}
		}
		return
	} else {
		panic("Either Table or Select should be specified")
	}
	return
}

func (f *Fiber) ScheduledPull() {
	panic("implement me!")
}
