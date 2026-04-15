package ginUtils

import (
	"fmt"
	"github.com/gin-gonic/gin"
)

func PrintRoutes(engine *gin.Engine, domain string) {
	for _, route := range engine.Routes() {
		fmt.Printf("Method: %s, Path: %s, Full URL: %s%s, Handlers: %v\n", route.Method, route.Path, domain, route.Path, route.Handler)
		//fmt.Printf("Method: %s, Path: %s, Handlers: %v\n", route.Method, route.Path, route.Handler)
	}
}
