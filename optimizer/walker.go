package optimizer

import (
	"fmt"
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	log "github.com/sirupsen/logrus"
)

type AstWalker struct {
	NodeCount    []int
	UnknownNodes []interface{}
	fn           func(ctx interface{}, node interface{}) (stop bool)
}

func (w *AstWalker) Walk(sql string, ctx interface{}) (ok bool, err error) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		return false, err
	}

	asts := make([]tree.NodeFormatter, len(stmts))
	for si, stmt := range stmts {
		asts[si] = stmt.AST
	}

	// nodeCount is incremented on each visited node per statement. It is
	// currently used to determine if walk is at the top-level statement
	// or not.
	var walk func(...interface{})
	walk = func(nodes ...interface{}) {
		for _, node := range nodes {
			w.NodeCount[0]++
			if w.fn != nil {
				if w.fn(ctx, node) {
					break
				}
			}

			if node == nil {
				continue
			}
			if _, ok := node.(tree.Datum); ok {
				continue
			}

			switch node := node.(type) {
			case *tree.AliasedTableExpr:
				walk(node.Expr)
			case *tree.AndExpr:
				walk(node.Left, node.Right)
			case *tree.AnnotateTypeExpr:
				walk(node.Expr)
			case *tree.Array:
				walk(node.Exprs)
			case *tree.BinaryExpr:
				walk(node.Left, node.Right)
			case *tree.CaseExpr:
				walk(node.Expr, node.Else)
				for _, w := range node.Whens {
					walk(w.Cond, w.Val)
				}
			case *tree.RangeCond:
				walk(node.Left, node.From, node.To)
			case *tree.CastExpr:
				walk(node.Expr)
			case *tree.CoalesceExpr:
				for _, expr := range node.Exprs {
					walk(expr)
				}
			case *tree.ColumnTableDef:
			case *tree.ComparisonExpr:
				walk(node.Left, node.Right)
			case *tree.CreateTable:
				for _, def := range node.Defs {
					walk(def)
				}
				if node.AsSource != nil {
					walk(node.AsSource)
				}
			case *tree.CTE:
				walk(node.Stmt)
			case *tree.DBool:
			case tree.Exprs:
				for _, expr := range node {
					walk(expr)
				}
			case *tree.FamilyTableDef:
			case *tree.FuncExpr:
				if node.WindowDef != nil {
					walk(node.WindowDef)
				}
				walk(node.Exprs, node.Filter)
			case *tree.IndexTableDef:
			case *tree.JoinTableExpr:
				walk(node.Left, node.Right, node.Cond)
			case *tree.NotExpr:
				walk(node.Expr)
			case *tree.NumVal:
			case *tree.OnJoinCond:
				walk(node.Expr)
			case *tree.OrExpr:
				walk(node.Left, node.Right)
			case *tree.ParenExpr:
				walk(node.Expr)
			case *tree.ParenSelect:
				walk(node.Select)
			case *tree.RowsFromExpr:
				for _, expr := range node.Items {
					walk(expr)
				}
			case *tree.Select:
				if node.With != nil {
					walk(node.With)
				}
				walk(node.Select)
				if node.OrderBy != nil {
					for _, order := range node.OrderBy {
						walk(order)
					}
				}
				if node.Limit != nil {
					walk(node.Limit)
				}
			case *tree.Order:
				walk(node.Expr, node.Table)
			case *tree.Limit:
				walk(node.Count)
			case *tree.SelectClause:
				walk(node.Exprs)
				if node.Where != nil {
					walk(node.Where)
				}
				if node.Having != nil {
					walk(node.Having)
				}
				for _, table := range node.From.Tables {
					walk(table)
				}
				if node.DistinctOn != nil {
					for _, distinct := range node.DistinctOn {
						walk(distinct)
					}
				}
				if node.GroupBy != nil {
					for _, group := range node.GroupBy {
						walk(group)
					}
				}
			case tree.SelectExpr:
				walk(node.Expr)
			case tree.SelectExprs:
				for _, expr := range node {
					walk(expr)
				}
			case *tree.SetVar:
				for _, expr := range node.Values {
					walk(expr)
				}
			case *tree.StrVal:
			case *tree.Subquery:
				walk(node.Select)
			case *tree.TableName:
			case *tree.Tuple:
				for _, expr := range node.Exprs {
					walk(expr)
				}
			case *tree.UnaryExpr:
				walk(node.Expr)
			case *tree.UniqueConstraintTableDef:
			case *tree.UnionClause:
				walk(node.Left, node.Right)
			case tree.UnqualifiedStar:
			case *tree.UnresolvedName:
			case *tree.ValuesClause:
				for _, row := range node.Rows {
					walk(row)
				}
			case *tree.Where:
				walk(node.Expr)
			case *tree.WindowDef:
				walk(node.Partitions)
				if node.Frame != nil {
					walk(node.Frame)
				}
			case *tree.WindowFrame:
				if node.Bounds.StartBound != nil {
					walk(node.Bounds.StartBound)
				}
				if node.Bounds.EndBound != nil {
					walk(node.Bounds.EndBound)
				}
			case *tree.WindowFrameBound:
				walk(node.OffsetExpr)
			case *tree.Window:
			case *tree.With:
				for _, expr := range node.CTEList {
					walk(expr)
				}
			default:
				w.UnknownNodes = append(w.UnknownNodes, node)
			}
		}
	}

	for _, ast := range asts {
		walk(ast)
	}

	return true, nil
}

func isColumn(node interface{}) bool {
	switch node.(type) {
	case tree.VariableExpr:
		return true
	}
	return false
}

func ColNamesInSelect2(sql string) (referredCols ReferredCols, err error) {
	referredCols = make(ReferredCols, 0)

	w := &AstWalker{
		NodeCount:    make([]int, 1),
		UnknownNodes: make([]interface{}, 0),
		fn: func(ctx interface{}, node interface{}) (stop bool) {
			rCols := ctx.(ReferredCols)
			if isColumn(node) {
				nodeName := fmt.Sprint(node)
				tableCols := strings.Split(nodeName, ".")
				colName := tableCols[len(tableCols)-1]
				rCols[colName] = 1
			}
			return false
		},
	}
	_, err = w.Walk(sql, referredCols)
	for _, col := range w.UnknownNodes {
		log.Warnf("unhandled column type %T", col)
	}
	return
}
