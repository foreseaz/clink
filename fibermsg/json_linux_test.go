//+build linux

package fibermsg

import (
	"testing"

	"github.com/auxten/clink/core"
	"github.com/auxten/clink/ngnx"
	. "github.com/smartystreets/goconvey/convey"
)

func TestMsgToSQLRow(t *testing.T) {
	Convey("msg to sql", t, func() {
		s, err := core.LoadConf("../test/mj/schema_test.yaml")
		So(err, ShouldBeNil)
		msg := &JsonMsg{
			Value: []byte(`{"after":{"TANS_AMT":"100.01","TRANS_FLAG":"P","TRANS_DATE":"2001-03-08 23:21:00",
				"TRANS_BRAN_CODE":"11670103","MC_TRSCODE":"CWD"},"rowid":"623481","scntime":984064860,"optype":"INSERT","name":"MJ_JOUR"}`),
			Table:       &s.Tables[0],
			DMLTypePath: s.Tables[0].KafkaSrc.OptTypePath,
		}

		eng := ngnx.NewEngine("row", "row", s)
		So(msg.ToDML(eng), ShouldResemble, "INSERT INTO mj (`rowid`,`scntime`,`TANS_AMT`,`TRANS_FLAG`,`TRANS_DATE`,`TRANS_BRAN_CODE`,`MC_TRSCODE`) VALUES ('623481',984064860,100.01,'P','2001-03-08 23:21:00','11670103','CWD');")

		msg.Value = []byte(`{"rowid":"623481","scntime":984064861,"optype":"UPDATE",
			"name":"MJ_JOUR","after":{"TRANS_FLAG":"0"},"before":{"TRANS_FLAG":"p"}}`)

		So(msg.ToDML(eng), ShouldResemble, "UPDATE mj SET `scntime` = 984064861, `TRANS_FLAG` = '0' WHERE `rowid` = '623481';")
	})
}

func TestMsgToSQLCol(t *testing.T) {
	Convey("msg to sql", t, func() {
		s, err := core.LoadConf("../test/mj/schema_test_ngncol.yaml")
		So(err, ShouldBeNil)
		msg := &JsonMsg{
			Value: []byte(`{"after":{"TANS_AMT":"100.01","TRANS_FLAG":"P","TRANS_DATE":"2001-03-08 23:21:00",
				"TRANS_BRAN_CODE":"11670103","MC_TRSCODE":"CWD"},"rowid":"623481","scntime":984064860,"optype":"INSERT","name":"MJ_JOUR"}`),
			Table:       &s.Tables[0],
			DMLTypePath: s.Tables[0].KafkaSrc.OptTypePath,
		}

		eng := ngnx.NewEngine("col", "col", s)
		So(msg.ToDML(eng), ShouldResemble, "INSERT INTO mj (rowid,scntime,TANS_AMT,TRANS_FLAG,TRANS_DATE,TRANS_BRAN_CODE,MC_TRSCODE) VALUES ('623481',984064860,100.01,'P','2001-03-08 23:21:00','11670103','CWD');")

		msg.Value = []byte(`{"rowid":"623481","scntime":984064861,"optype":"UPDATE",
			"name":"MJ_JOUR","after":{"TRANS_FLAG":"0"},"before":{"TRANS_FLAG":"p"}}`)

		So(msg.ToDML(eng), ShouldResemble, "UPDATE mj SET scntime = 984064861, TRANS_FLAG = '0' WHERE rowid = '623481';")
	})
}
