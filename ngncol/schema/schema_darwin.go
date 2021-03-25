//+build darwin

package schema

import (
	"github.com/auxten/clink/core"
)

func GetDDL(t *core.Table) (ddl []string) {
	panic("ngncol not supported under darwin")
}
