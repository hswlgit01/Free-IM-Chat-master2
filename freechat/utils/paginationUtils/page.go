package paginationUtils

import (
	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
)

// DepPagination 后续接口请求，返回参数 返回都是下划线
type DepPagination struct {
	Page     int32  `json:"page" form:"page"`
	PageSize int32  `json:"page_size" form:"page_size"`
	Order    string `json:"order" form:"order"`
}

func QueryToDepPagination(c *gin.Context) (*DepPagination, error) {
	// 添加兼容处理
	page := &DepPagination{}
	page.Order = c.Query("order")

	pageParam := c.Query("page")
	pageSizeParam := c.Query("pageSize")
	if pageSizeParam == "" {
		pageSizeParam = c.Query("page_size")
	}

	if pageParam != "" {
		pageInt32, err := strconv.Atoi(pageParam)
		if err != nil {
			return nil, freeErrors.PageParameterInvalidErr
		}
		page.Page = int32(pageInt32)
	}

	if pageSizeParam != "" {
		pageSizeInt32, err := strconv.Atoi(pageSizeParam)
		if err != nil {
			return nil, freeErrors.PageParameterInvalidErr
		}
		page.PageSize = int32(pageSizeInt32)
	}

	return page, nil
}

func (p *DepPagination) GetPageNumber() int32 {
	return p.Page
}

func (p *DepPagination) GetShowNumber() int32 {
	return p.PageSize
}

func (p *DepPagination) ToOptions() *options.FindOptions {
	opts := options.Find()

	offset := (p.Page - 1) * p.PageSize
	if offset >= 0 {
		//skip := p.Page-1 * p.PageSize
		opts = options.Find().SetSkip(int64(offset))
	}

	if p.PageSize > 0 {
		opts = opts.SetLimit(int64(p.PageSize))
	}

	if p.Order != "" {
		opts = opts.SetSort(bson.M{p.Order: -1})
	}
	return opts
}

func (p *DepPagination) ToBsonMList() []bson.M {
	result := make([]bson.M, 0)

	offset := (p.Page - 1) * p.PageSize
	if offset >= 0 {
		result = append(result,
			bson.M{"$skip": (p.Page - 1) * p.PageSize},
		)
	}

	if p.PageSize > 0 {
		result = append(result,
			bson.M{"$limit": p.PageSize},
		)
	}

	if p.Order != "" {
		result = append(result, bson.M{"$sort": bson.M{p.Order: -1}})
	}
	return result
}

type ListResp[T any] struct {
	Total int64 `json:"total"`
	List  []T   `json:"data"`
}
