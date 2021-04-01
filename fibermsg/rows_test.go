package fibermsg

import (
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/auxten/clink/ngncol"
	"github.com/auxten/clink/ngnrow"
)

func TestRows(t *testing.T) {
	Convey("rows DML and args", t, func() {
		start, _ := time.Parse("2006-01-02 15:04:05", "2006-01-02 15:04:05")
		rows := Rows{
			TableName: "testRowTable",
			ColNames:  []string{"col_int", "col_float", "col_string", "col_bytes", "col_bool", "col_time"},
			Rows: [][]interface{}{
				{1, 1.1, "str1", []byte{65, 66}, true, start},
				{2, 2.2, "str2", []byte{67, 68}, false, start.Add(time.Hour)},
				{3, 3.3, "str3", []byte{69}, false, start.Add(2 * time.Hour)},
			},
		}
		sql := rows.ToDML(&ngnrow.Engine{})
		So(sql, ShouldResemble, "INSERT INTO testRowTable (`col_int`,`col_float`,`col_string`,`col_bytes`,`col_bool`,`col_time`) VALUES (?, ?, ?, ?, ?, ?);")
		args := rows.DMLArgs(&ngnrow.Engine{})
		So(fmt.Sprintf("%s", args[2]), ShouldResemble, "[%!s(int=3) %!s(float64=3.3) str3 E %!s(bool=false) 2006-01-02 17:04:05 +0000 UTC]")

		sql = rows.ToDML(&ngncol.Engine{})
		So(sql, ShouldResemble, "INSERT INTO testRowTable (col_int,col_float,col_string,col_bytes,col_bool,col_time) VALUES (1,1.100000,'str1','\\x4142'::BLOB,TRUE,'2006-01-02 15:04:05'),(2,2.200000,'str2','\\x4344'::BLOB,FALSE,'2006-01-02 16:04:05'),(3,3.300000,'str3','\\x45'::BLOB,FALSE,'2006-01-02 17:04:05');")
		args = rows.DMLArgs(&ngncol.Engine{})
		So(args, ShouldBeNil)

		So(rows.String(), ShouldResemble, "columns: [col_int col_float col_string col_bytes col_bool col_time]\n@3 rows")
	})
}
