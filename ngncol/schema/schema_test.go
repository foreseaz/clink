package schema

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/auxten/clink/core"
)

func TestDDL(t *testing.T) {
	Convey("Table schema to DDL", t, func() {
		conf, err := core.LoadConf("../../test/atmj/schema_test_ngncol.yaml")
		So(err, ShouldBeNil)

		ddl := GetDDL(&conf.Tables[0])

		So(ddl[0], ShouldResemble,
			`CREATE TABLE IF NOT EXISTS atmj (
rowid string NOT NULL,
scntime bigint,
TANS_AMT bigint DEFAULT 0,
TRANS_FLAG string,
TRANS_DATE date,
TRANS_BRAN_CODE string,
ATMC_TRSCODE string
);`)
		So(ddl[1], ShouldResemble, "CREATE INDEX IF NOT EXISTS idx__atmj__TRANS_FLAG ON atmj (TRANS_FLAG);")
		So(ddl[2], ShouldResemble, "CREATE INDEX IF NOT EXISTS idx__atmj__TRANS_DATE ON atmj (TRANS_DATE);")
	})
}
