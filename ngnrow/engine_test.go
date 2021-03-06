package ngnrow

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/auxten/clink/core"
)

func TestDDL(t *testing.T) {
	Convey("Table schema to DDL", t, func() {
		conf, err := core.LoadConf("../test/mj/schema_test.yaml")
		So(err, ShouldBeNil)
		eng := Engine{}
		ddl := eng.GetDDL(&conf.Tables[0])
		So(ddl[0], ShouldResemble,
			`CREATE TABLE IF NOT EXISTS 'mj' (
'rowid' string PRIMARY KEY NOT NULL,
'scntime' bigint,
'TANS_AMT' bigint DEFAULT 0,
'TRANS_FLAG' string,
'TRANS_DATE' date,
'TRANS_BRAN_CODE' string,
'MC_TRSCODE' string
);`)
		So(ddl[1], ShouldResemble, "CREATE INDEX IF NOT EXISTS 'idx__mj__TRANS_FLAG' ON `mj` (`TRANS_FLAG`);")
		So(ddl[2], ShouldResemble, "CREATE INDEX IF NOT EXISTS 'idx__mj__TRANS_DATE' ON `mj` (`TRANS_DATE`);")
	})
}
