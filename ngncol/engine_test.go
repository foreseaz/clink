//+build linux

package ngncol

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
		s, err := core.LoadConf("../test/mj/schema_test_ngncol.yaml")
		So(err, ShouldBeNil)

		eng := Engine{
			Name:   "mj",
			Type:   "clink",
			Store:  ":memory:",
			Schema: s,
		}
		So(err, ShouldBeNil)
		err = eng.InitTables()
		So(err, ShouldBeNil)
		schemaStr, err := eng.ShowSchema()
		So(err, ShouldBeNil)
		So(schemaStr, ShouldResemble,
			`CREATE TABLE 'mj' (
'rowid' string PRIMARY KEY NOT NULL,
'scntime' bigint,
'TANS_AMT' bigint DEFAULT 0,
'TRANS_FLAG' string,
'TRANS_DATE' date,
'TRANS_BRAN_CODE' string,
'MC_TRSCODE' string
)`)
		indexStr, err := eng.ShowIndex()
		So(err, ShouldBeNil)
		So(indexStr, ShouldResemble, "CREATE INDEX 'idx__mj__TRANS_FLAG' ON `mj` (`TRANS_FLAG`);\nCREATE INDEX 'idx__mj__TRANS_DATE' ON `mj` (`TRANS_DATE`);\n")
		f, err := os.Open("../test/mj/mj_msg_1000_test.txt")
		So(err, ShouldBeNil)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			err = eng.Exec("mj", sc.Text())
			So(err, ShouldBeNil)
		}
		result, err := eng.Query(eng.Schema.Query)
		So(err, ShouldBeNil)
		fmt.Printf("Result %s", result)
	})
}
