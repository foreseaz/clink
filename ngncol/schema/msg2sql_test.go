package schema

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMsgToSQL(t *testing.T) {
	Convey("msg to sql", t, func() {
		s, err := LoadConf("../../test/atmj/schema_test_ngncol.yaml")
		So(err, ShouldBeNil)
		msg := &Msg{
			Value: []byte(`{"after":{"TANS_AMT":"100.01","TRANS_FLAG":"P","TRANS_DATE":"2001-03-08 23:21:00",
				"TRANS_BRAN_CODE":"11670103","ATMC_TRSCODE":"CWD"},"rowid":"623481","scntime":984064860,"optype":"INSERT","name":"ATMJ_JOUR"}`),
			Table: &s.Tables[0],
		}

		So(msg.ToSQL(), ShouldResemble, "INSERT INTO atmj (rowid,scntime,TANS_AMT,TRANS_FLAG,TRANS_DATE,TRANS_BRAN_CODE,ATMC_TRSCODE) VALUES ('623481',984064860,100.01,'P','2001-03-08 23:21:00','11670103','CWD');")

		msg.Value = []byte(`{"rowid":"623481","scntime":984064861,"optype":"UPDATE",
			"name":"ATMJ_JOUR","after":{"TRANS_FLAG":"0"},"before":{"TRANS_FLAG":"p"}}`)

		So(msg.ToSQL(), ShouldResemble, "UPDATE atmj SET scntime = 984064861, TRANS_FLAG = '0' WHERE rowid = '623481';")
	})
}
