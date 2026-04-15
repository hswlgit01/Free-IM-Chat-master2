package ginUtils

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/utils"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strconv"
	"strings"
	"time"
)

// QueryToCstTime 解析查询参数中的时间戳为中国标准时间(CST)
// 将Unix时间戳转换为CST时区的time.Time对象
func QueryToCstTime(c *gin.Context, timeParam string) (time.Time, error) {
	queryStr := strings.TrimSpace(c.Query(timeParam))

	if queryStr != "" {
		timeInt, err := strconv.ParseInt(queryStr, 10, 64)
		if err != nil {
			return time.Time{}, freeErrors.ParameterInvalidErr
		}
		return time.Unix(timeInt, 0).In(utils.CST), nil
	}
	return time.Time{}, nil
}

// QueryToUtcTime 解析查询参数中的时间戳
// 注意：2024-01-12修改，现在使用中国标准时间(CST)而不是UTC
// 此函数名称与实际行为不匹配，保留是为了向后兼容
// 建议新代码使用 QueryToCstTime 函数
// @deprecated - 请使用 QueryToCstTime 替代
func QueryToUtcTime(c *gin.Context, utcTime string) (time.Time, error) {
	return QueryToCstTime(c, utcTime)
}

func QueryToObjectId(c *gin.Context, name string) (primitive.ObjectID, error) {
	queryStr := strings.TrimSpace(c.Query(name))
	if queryStr == "" {
		return primitive.NilObjectID, nil
	}

	objId, err := primitive.ObjectIDFromHex(queryStr)
	if err != nil {
		return primitive.NilObjectID, freeErrors.ParameterInvalidErr
	}
	return objId, nil
}

func QueryToObjectIds(c *gin.Context, name string) ([]primitive.ObjectID, error) {
	queryStr := c.Query(name)

	objIds := make([]primitive.ObjectID, 0)
	objIdsStrings := strings.Split(queryStr, ",")
	for _, objIdStr := range objIdsStrings {
		if objIdStr == "" {
			continue
		}

		objId, err := primitive.ObjectIDFromHex(objIdStr)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr
		}
		objIds = append(objIds, objId)
	}

	return objIds, nil
}
