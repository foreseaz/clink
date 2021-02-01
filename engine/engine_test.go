package engine

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/auxten/clink/schema"
)

func TestEngine(t *testing.T) {
	Convey("New engine", t, func() {
		s, err := schema.LoadConf("../test/atmj/schema_test.yaml")
		So(err, ShouldBeNil)

		eng := NewEngine("atmj", s)
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
		So(indexStr, ShouldResemble, "CREATE INDEX 'idx__atmj__TRANS_FLAG' ON `atmj` (`TRANS_FLAG`);\nCREATE INDEX 'idx__atmj__TRANS_DATE' ON `atmj` (`TRANS_DATE`);\n")
		f, err := os.Open("../test/atmj/atmj_msg_1000_test.txt")
		So(err, ShouldBeNil)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			err = eng.Exec("atmj", sc.Bytes())
			So(err, ShouldBeNil)
		}
		rows, err := eng.Db.Query(eng.Schema.Query)
		So(err, ShouldBeNil)
		defer rows.Close()
		var (
			transDate     string
			transBranCode string
			balance       float64
			count         int64
		)

		for rows.Next() {
			err := rows.Scan(&transDate, &transBranCode, &balance, &count)
			So(err, ShouldBeNil)
			fmt.Printf("TRANS_DATE %s\n", transDate)
			fmt.Printf("TRANS_BRAN_CODE %s\n", transBranCode)
			fmt.Printf("BALANCE %f\n", balance)
			fmt.Printf("CNT %d\n", count)
		}
	})
}
