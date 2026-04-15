package networkRoute

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/tools/apiresp"
	"time"
)

type NetworkRouteCtl struct{}

func NewNetworkRouteCtl() *NetworkRouteCtl {
	return &NetworkRouteCtl{}
}

func (n *NetworkRouteCtl) TestPing(c *gin.Context) {
	apiresp.GinSuccess(c, gin.H{
		"timestamp": time.Now().Unix(),
		"status":    "ok",
	})
}
