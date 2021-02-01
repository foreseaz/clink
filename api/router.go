package api

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/gin-gonic/gin"

	"github.com/auxten/clink/engine"
	"github.com/auxten/clink/utils"
)

func newRouter(eng *engine.Engine) (router *gin.Engine) {
	router = gin.Default()
	router.GET("/me", func(c *gin.Context) {
		c.String(http.StatusOK, "Welcome Clink Server")
	})
	//{ // proxy to npm dev server
	//	router.NoRoute(proxyTo("http://localhost:8080"))
	//}

	{ // v1 version api
		v1 := router.Group("/v1")
		{
			v1.Any("/db/query", QueryHandler(eng))
			v1.Any("/db/exec", exec)
		}
	}
	return
}

func proxyTo(upstream string) func(*gin.Context) {
	remote, err := url.Parse(upstream)
	utils.CheckErrFatal(err)

	return func(c *gin.Context) {
		proxy := httputil.NewSingleHostReverseProxy(remote)
		proxy.Director = func(req *http.Request) {
			req.Header = c.Request.Header
			req.Host = remote.Host
			req.URL.Scheme = remote.Scheme
			req.URL.Host = remote.Host
			req.URL.Path = c.Request.URL.RequestURI()
		}

		proxy.ServeHTTP(c.Writer, c.Request)
	}
}
