package api

import (
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/auxten/clink/core"
)

// Query defines single query.
type Query struct {
	Query string        `form:"q"`
	Args  []interface{} `form:"Args"`
}

type QueryResult struct {
	result  [][]interface{} `json:"result"`
	columns []string        `json:"columns"`
}

func QueryHandler(eng core.Engine) func(*gin.Context) {
	return func(c *gin.Context) {
		var (
			err   error
			query Query
			qr    QueryResult
		)
		if err = c.ShouldBind(&query); err != nil {
			log.WithError(err).Errorf("processing %v", c.Request)
			c.PureJSON(400, err)
			return
		}

		if qr.columns, qr.result, err = eng.Query(query.Query, query.Args...); err != nil {
			log.WithError(err).Errorf("marshal rows to json")
			c.PureJSON(500, err)
			return
		}

		c.PureJSON(200, qr)
	}
}

//func exec(c *gin.Context) {
//	var (
//		images *gjson.Result
//		err    error
//	)
//	reg := c.Param("reg")
//	proto := c.Param("proto")
//	regAddr := fmt.Sprintf("%s://%s", proto, reg)
//	if err != nil {
//		log.Errorf("get images from %s failed: %v", regAddr, err)
//		c.PureJSON(500, err)
//		return
//	}
//
//	c.PureJSON(200, images.Value())
//}
