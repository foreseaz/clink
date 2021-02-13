package ngnrow

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/auxten/clink/core"
)

func TestEngine(t *testing.T) {
	Convey("New engine", t, func() {
		s, err := core.LoadConf("../test/atmj/schema_test.yaml")
		So(err, ShouldBeNil)

		eng := Engine{
			Name:   "atmj",
			Type:   "sqlite3",
			Store:  ":memory:",
			Schema: s,
		}
		So(err, ShouldBeNil)
		err = eng.InitTables()
		So(err, ShouldBeNil)
		schemaStr, err := eng.ShowSchema()
		So(err, ShouldBeNil)
		So(schemaStr, ShouldResemble,
			`CREATE TABLE 'atmj' (
'rowid' string PRIMARY KEY NOT NULL,
'scntime' bigint,
'TANS_AMT' bigint DEFAULT 0,
'TRANS_FLAG' string,
'TRANS_DATE' date,
'TRANS_BRAN_CODE' string,
'ATMC_TRSCODE' string
)`)
		indexStr, err := eng.ShowIndex()
		So(err, ShouldBeNil)
		So(indexStr, ShouldResemble, "CREATE INDEX 'idx__atmj__TRANS_FLAG' ON `atmj` (`TRANS_FLAG`);\nCREATE INDEX 'idx__atmj__TRANS_DATE' ON `atmj` (`TRANS_DATE`);\nCREATE INDEX 'idx__atmj__TRANS_BRAN_CODE' ON `atmj` (`TRANS_BRAN_CODE`);\n")
		f, err := os.Open("../test/atmj/atmj_msg_1000_test.txt")
		So(err, ShouldBeNil)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			err = eng.Exec("atmj", sc.Text())
			So(err, ShouldBeNil)
		}
		result, err := eng.Query(eng.Schema.Query)
		So(err, ShouldBeNil)
		fmt.Printf("Result %s", result)
	})
}
