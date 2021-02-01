package api

import (
	"database/sql"
	"fmt"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"github.com/auxten/clink/engine"
	"github.com/auxten/clink/utils"
)

// Query defines single query.
type Query struct {
	Query string        `form:"q"`
	Args  []interface{} `form:"Args"`
}

func QueryHandler(eng *engine.Engine) func(*gin.Context) {
	return func(c *gin.Context) {
		var (
			err           error
			query         Query
			rows          *sql.Rows
			generalResult [][]interface{}
		)
		if err = c.ShouldBind(&query); err != nil {
			log.WithError(err).Errorf("processing %v", c.Request)
			c.PureJSON(400, err)
			return
		}
		log.Debugf("Query: %v", query)
		if rows, err = eng.Db.Query(query.Query, query.Args...); err != nil {
			log.WithError(err).Errorf("query %s with Args %v", query.Query, query.Args)
			c.PureJSON(500, err)
			return
		}
		defer rows.Close()
		if generalResult, err = utils.ReadAllRows(rows); err != nil {
			log.WithError(err).Errorf("marshal rows to json")
			c.PureJSON(500, err)
			return
		}

		c.PureJSON(200, generalResult)
	}
}

func exec(c *gin.Context) {
	var (
		images *gjson.Result
		err    error
	)
	reg := c.Param("reg")
	proto := c.Param("proto")
	regAddr := fmt.Sprintf("%s://%s", proto, reg)
	if err != nil {
		log.Errorf("get images from %s failed: %v", regAddr, err)
		c.PureJSON(500, err)
		return
	}

	c.PureJSON(200, images.Value())
}
