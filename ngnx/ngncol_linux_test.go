//+build linux

package ngnx

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/auxten/clink/core"
	"github.com/auxten/clink/fibermsg"
	"github.com/auxten/clink/ngncol"
)

func TestEngine(t *testing.T) {
	Convey("New engine", t, func() {
		s, err := core.LoadConf("../test/mj/schema_test_ngncol.yaml")
		So(err, ShouldBeNil)

		eng := ngncol.Engine{
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
			"CREATE TABLE mj(rowid VARCHAR NOT NULL, scntime BIGINT, tans_amt BIGINT DEFAULT(0), trans_flag VARCHAR, trans_date DATE, trans_bran_code VARCHAR, mc_trscode VARCHAR);")
		indexStr, err := eng.ShowIndex()
		So(err, ShouldBeNil)
		So(
			indexStr,
			ShouldResemble,
			`CREATE INDEX IF NOT EXISTS idx__mj__TRANS_BRAN_CODE ON mj (TRANS_BRAN_CODE);;
CREATE INDEX IF NOT EXISTS idx__mj__TRANS_FLAG ON mj (TRANS_FLAG);;
CREATE INDEX IF NOT EXISTS idx__mj__TRANS_DATE ON mj (TRANS_DATE);;
`)
		f, err := os.Open("../test/mj/mj_msg_1000_test.txt")
		So(err, ShouldBeNil)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			msg := &fibermsg.JsonMsg{
				Value:       sc.Bytes(),
				Table:       &s.Tables[0],
				DMLTypePath: s.Tables[0].KafkaSrc.OptTypePath,
			}

			err = eng.Exec(msg)
			So(err, ShouldBeNil)
		}
		result, err := eng.Query(eng.Schema.Query)
		So(err, ShouldBeNil)
		fmt.Printf("Result %s", result)
	})
}
