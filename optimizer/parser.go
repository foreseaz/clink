package optimizer

import (
	"errors"
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

func sql2ast(sql string) (parser.Statements, error) {
	return parser.Parse(sql)
}

// isVar returns true if the expression's value can vary during plan
// execution. The parameter allowConstPlaceholders should be true
// in the common case of scalar expressions that will be evaluated
// in the context of the execution of a prepared query, where the
// placeholder will have the same value for every row processed.
// It is set to false for scalar expressions that are not
// evaluated as part of query execution, eg. DEFAULT expressions.
func isVar(evalCtx *tree.EvalContext, expr tree.Expr, allowConstPlaceholders bool) bool {
	//log.Debugf("Expr %s, type: %t", expr, expr)
	switch expr.(type) {
	case tree.VariableExpr:
		return true
	case *tree.Placeholder:
		if allowConstPlaceholders {
			if evalCtx == nil || !evalCtx.HasPlaceholders() {
				// The placeholder cannot be resolved -- it is variable.
				return true
			}
			return evalCtx.Placeholders.IsUnresolvedPlaceholder(expr)
		}
		// Placeholders considered always variable.
		return true
	}
	return false
}

type referredVarsVisitor struct {
	referredVars tree.Exprs
}

func (v *referredVarsVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if isVar(nil, expr, false /*allowConstPlaceholders*/) {
		v.referredVars = append(v.referredVars, expr)
	}

	return true, expr
}

func (*referredVarsVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// ReferredVarsInExpr returns all variables contained in the expression.
// (variables = sub-expressions, placeholders, indexed vars, etc.)
func ReferredVarsInExpr(expr tree.Expr) tree.Exprs {
	v := referredVarsVisitor{}
	tree.WalkExprConst(&v, expr)
	return v.referredVars
}

type ReferredCols map[string]int

// ColNamesInSelect finds all referred variables in a Select Statement.
// (variables = sub-expressions, placeholders, indexed vars, etc.)
// Implementation limits:
//  1. WITH clause is not supported.
//	2. Table with AS is not normalized.
func ColNamesInSelect(sel *tree.Select) (cols ReferredCols, err error) {
	v := referredVarsVisitor{}
	if sel.With != nil {
		return nil, errors.New("WITH statement not supported")
	}
	if sel.Select != nil {
		treeSelectClause := sel.Select.(*tree.SelectClause)
		if treeSelectClause.Exprs != nil {
			for _, expr := range treeSelectClause.Exprs {
				tree.WalkExprConst(&v, expr.Expr)
			}
		}
		if treeSelectClause.DistinctOn != nil {
			for _, dist := range treeSelectClause.DistinctOn {
				tree.WalkExprConst(&v, dist)
			}
		}
		if treeSelectClause.From.Tables != nil {
			//for _, t := range treeSelectClause.From.Tables {
			//	tree.WalkExprConst(&v, t)
			//}
		}
	}
	if sel.OrderBy != nil {
		for _, order := range sel.OrderBy {
			tree.WalkExprConst(&v, order.Expr)
		}
	}
	if sel.Limit != nil {
		if sel.Limit.Count != nil {
			tree.WalkExprConst(&v, sel.Limit.Count)
		}
		if sel.Limit.Offset != nil {
			tree.WalkExprConst(&v, sel.Limit.Offset)
		}
	}

	cols = make(ReferredCols, len(v.referredVars))
	for _, col := range v.referredVars {
		tableCols := strings.Split(col.String(), ".")
		colName := tableCols[len(tableCols)-1]
		if _, exist := cols[colName]; exist {
			cols[colName] += 1
		} else {
			cols[colName] = 0
		}

	}
	return cols, nil
}
