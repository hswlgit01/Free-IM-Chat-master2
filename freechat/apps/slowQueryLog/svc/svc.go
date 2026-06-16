package svc

import (
	"context"
	"strings"

	"github.com/openimsdk/chat/freechat/apps/slowQueryLog/dto"
	"github.com/openimsdk/chat/freechat/apps/slowQueryLog/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
)

const defaultSlowQueryDurationMS int64 = 3000

type SlowQueryLogSvc struct{}

func NewSlowQueryLogSvc() *SlowQueryLogSvc {
	return &SlowQueryLogSvc{}
}

// dawn 2026-06-16 新增慢查询日志后台查询：按条件分页返回超过阈值的 Mongo 查询记录。
func (s *SlowQueryLogSvc) CmsList(ctx context.Context, req dto.ListSlowQueryLogReq, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.SlowQueryLogResp], error) {
	dao := model.NewSlowQueryLogDao(plugin.MongoCli().GetDB())
	minDuration := req.MinDurationMS
	if minDuration <= 0 {
		minDuration = defaultSlowQueryDurationMS
	}
	total, rows, err := dao.Search(ctx, model.SlowQueryLogSearchFilter{
		Keyword:       strings.TrimSpace(req.Keyword),
		Collection:    strings.TrimSpace(req.Collection),
		Operation:     strings.TrimSpace(req.Operation),
		MinDurationMS: minDuration,
		StartTime:     req.StartTime,
		EndTime:       req.EndTime,
	}, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.SlowQueryLogResp]{
		Total: total,
		List:  make([]*dto.SlowQueryLogResp, 0, len(rows)),
	}
	for _, row := range rows {
		resp.List = append(resp.List, dto.NewSlowQueryLogResp(row))
	}
	return resp, nil
}
