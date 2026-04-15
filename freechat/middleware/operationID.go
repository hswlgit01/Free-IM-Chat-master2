package middleware

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/protocol/constant"
)

func GinParseOperationID() gin.HandlerFunc {
	return func(c *gin.Context) {
		operationID := c.Request.Header.Get(constant.OperationID)
		if operationID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"errCode": 1001,
				"errMsg":  "ArgsError",
				"errDlt":  "header must have operationID",
			})
			c.Abort()
			return
		}
		c.Set(constant.OperationID, operationID)
		c.Next()
	}
}

func GetOperationId(c *gin.Context) (string, error) {
	operationID := c.GetString(constant.OperationID)
	if operationID == "" {
		return "", errors.New("failed to get operationID from context")
	}
	return operationID, nil

}
