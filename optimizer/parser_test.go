package optimizer

import (
	"sort"
	"testing"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	log "github.com/sirupsen/logrus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestParser(t *testing.T) {
	Convey("SQL to AST", t, func() {
		log.SetLevel(log.DebugLevel)
		const sql = "SELECT SUBSTR(t1.TRANS_DATE, 0, 10) as trans_date, t1.TRANS_BRAN_CODE as trans_bran_code,ROUND(SUM(t1.TANS_AMT)/10000,2) as balance, count(t1.rowid) as cnt FROM atmj t1 WHERE t1.ATMC_TRSCODE in ('INQ', 'LIS', 'CWD', 'CDP', 'TFR', 'PIN', 'REP', 'PAY') AND t1.TRANS_FLAG = '0' GROUP BY SUBSTR(t1.TRANS_DATE, 0, 10),t1.TRANS_BRAN_CODE ORDER by trans_date;"
		s, err := sql2ast(sql)
		So(err, ShouldBeNil)
		//for _, ast := range s {
		//	log.Debugf("ast: %#v, %s, %t", ast.AST, ast.AST.StatementTag(), ast.AST)
		//	fmt.Println(tree.Pretty(ast.AST))
		//}
		// tree.Select represents a SelectStatement with an ORDER and/or LIMIT.
		treeSelect := s[0].AST.(*tree.Select)
		// tree.SelectClause represents a SELECT statement.
		treeSelectClause := treeSelect.Select.(*tree.SelectClause)

		referredCols, err := ColNamesInSelect(treeSelect)
		log.Debugf("referredCols %v", referredCols)
		So(err, ShouldBeNil)

		So(ReferredVarsInExpr(treeSelectClause.Exprs[0].Expr), ShouldResemble, "")
		So(s.String(), ShouldResemble, "")
		So(1, ShouldEqual, 1)
	})
}

func allColsContained(set ReferredCols, cols []string) bool {
	if cols == nil {
		if set == nil {
			return true
		} else {
			return false
		}
	}
	if len(set) != len(cols) {
		return false
	}
	for _, col := range cols {
		if _, exist := set[col]; !exist {
			return false
		}
	}
	return true
}

func TestReferredVarsInSelectStatement(t *testing.T) {
	log.SetLevel(log.WarnLevel)
	testCases := []struct {
		sql  string
		cols []string
		err  error
	}{
		{"SELECT a.r1, a.r2 FROM a ORDER BY a.r3 LIMIT 1", []string{"r1", "r2", "r3"}, nil},
		{"SELECT SUBSTR(t1.TRANS_DATE, 0, 10) as trans_date, t1.TRANS_BRAN_CODE as trans_bran_code,ROUND(SUM(t1.TANS_AMT)/10000,2) as balance, count(t1.rowid) as cnt FROM atmj t1 WHERE t1.ATMC_TRSCODE in ('INQ', 'LIS', 'CWD', 'CDP', 'TFR', 'PIN', 'REP', 'PAY') AND t1.TRANS_FLAG = '0' GROUP BY SUBSTR(t1.TRANS_DATE, 0, 10),t1.TRANS_BRAN_CODE ORDER by trans_date;", []string{"atmc_trscode", "rowid", "tans_amt", "trans_bran_code", "trans_date", "trans_flag"}, nil},
		{`
			SELECT count(DISTINCT s_i_id)
			FROM order_line
			JOIN stock
			ON s_i_id=ol_i_id AND s_w_id=ol_w_id
			WHERE ol_w_id = $1
				AND ol_d_id = $2
				AND ol_o_id BETWEEN $3 - 20 AND $3 - 1
				AND s_quantity < $4
		`, []string{"s_i_id", "ol_i_id", "s_w_id", "ol_w_id", "ol_d_id", "ol_o_id", "s_quantity"}, nil},
	}

	for _, tc := range testCases {
		Convey(tc.sql, t, func() {
			referredCols, err := func() (ReferredCols, error) {
				return ColNamesInSelect2(tc.sql)
			}()
			So(err, ShouldResemble, tc.err)
			cols := make([]string, 0, len(tc.cols))
			for k, _ := range referredCols {
				cols = append(cols, k)
			}
			sort.Strings(cols)
			sort.Strings(tc.cols)
			So(cols, ShouldResemble, tc.cols)
		})
	}
}
