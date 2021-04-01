package mysql

import (
	"context"
	"encoding/json"
	"testing"

	log "github.com/sirupsen/logrus"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/auxten/clink/core"
	"github.com/auxten/clink/ngnx"
)

func TestMysqlDataSource(t *testing.T) {
	Convey("MySQL to ngnrow", t, func() {
		log.SetLevel(log.DebugLevel)
		schm, err := core.LoadConf("../../test/mysql/ngnrow_test.yaml")
		So(err, ShouldBeNil)

		eng := ngnx.NewEngine(schm.Engine, "MySQL2Ngnrow", schm)
		err = eng.InitTables()
		So(err, ShouldBeNil)

		fiber := Fiber{
			Table: &schm.Tables[0],
			Eng:   eng,
		}

		err = fiber.Attach(context.Background())
		So(err, ShouldBeNil)

		err = fiber.InitTables()
		So(err, ShouldBeNil)

		err = fiber.Pull()
		So(err, ShouldBeNil)

		fiber.Detach()

		cols, results, err := eng.Query(schm.Query)
		So(err, ShouldBeNil)
		So(cols, ShouldResemble, []string{"area", "Location", "sum(Loan_amount)"})
		So(len(results), ShouldBeGreaterThan, 10)
		jb, _ := json.Marshal(results)
		log.Infof("%s", string(jb))
	})
}
