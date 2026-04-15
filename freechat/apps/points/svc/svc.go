package svc

import (
	"context"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"strconv"
	"time"

	OrgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/apps/points/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type PointsSvc struct{}

func NewPointsSvc() *PointsSvc {
	return &PointsSvc{}
}

// IssuePointsReq 内部发放积分请求体
type IssuePointsReq struct {
	ImServerUserId string             `json:"im_server_user_id" binding:"required" remark:"用户IM服务器ID"`
	OrganizationId primitive.ObjectID `json:"organization_id" binding:"required" remark:"组织ID"`
	Points         int64              `json:"points" binding:"required,min=1" remark:"积分数量"`
	PointsType     model.PointsType   `json:"points_type" binding:"required" remark:"积分类型"`
	Source         string             `json:"source,omitempty" remark:"来源"`
	Description    string             `json:"description,omitempty" remark:"积分发放描述"`
}

// QueryPointsRecordsReq 查询积分记录请求体
type QueryPointsRecordsReq struct {
	paginationUtils.DepPagination `json:",inline"`
	Keyword                       string            `json:"keyword,omitempty" remark:"用户关键词（account，昵称，用户imserverID）"`
	MinPoints                     *int64            `json:"min_points,omitempty" remark:"最小积分数量"`
	MaxPoints                     *int64            `json:"max_points,omitempty" remark:"最大积分数量"`
	PointsType                    *model.PointsType `json:"points_type,omitempty" remark:"积分类型"`
	StartTime                     string            `json:"start_time,omitempty" remark:"发放开始时间"`
	EndTime                       string            `json:"end_time,omitempty" remark:"发放结束时间"`
}

// PointsRecordWithUserInfoResp 积分记录响应体（包含用户信息）
type PointsRecordWithUserInfoResp struct {
	ID             primitive.ObjectID `json:"id"`
	Points         int64              `json:"points"`
	ImServerUserId string             `json:"im_server_user_id"`
	OrganizationId primitive.ObjectID `json:"organization_id"`
	PointsType     model.PointsType   `json:"points_type"`
	Source         string             `json:"source,omitempty"`
	Description    string             `json:"description,omitempty"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
	// 用户信息
	UserInfo      map[string]interface{} `json:"user_info,omitempty"`
	AttributeInfo map[string]interface{} `json:"attribute_info,omitempty"`
}

// IssuePoints 内部发放积分（记录积分并累加到用户总积分）
func (w *PointsSvc) IssuePoints(ctx context.Context, req *IssuePointsReq) error {
	pointsDao := model.NewPointsDao(plugin.MongoCli().GetDB())
	orgUserDao := OrgModel.NewOrganizationUserDao(plugin.MongoCli().GetDB())

	// 使用事务确保数据一致性
	return plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		// 1. 创建积分记录
		pointsRecord := &model.Points{
			Points:         req.Points,
			ImServerUserId: req.ImServerUserId,
			OrganizationId: req.OrganizationId,
			PointsType:     req.PointsType,
			Source:         req.Source,
			Description:    req.Description,
		}

		err := pointsDao.Create(sessionCtx, pointsRecord)
		if err != nil {
			return errs.Wrap(err)
		}

		// 2. 累加用户总积分
		err = orgUserDao.AddPointsByImServerUserId(sessionCtx, req.ImServerUserId, req.OrganizationId, req.Points)
		if err != nil {
			return errs.Wrap(err)
		}

		return nil
	})
}

// QueryPointsRecords 查询积分记录列表（管理端）
func (w *PointsSvc) QueryPointsRecords(ctx context.Context, req *QueryPointsRecordsReq, orgID primitive.ObjectID) (*paginationUtils.ListResp[*PointsRecordWithUserInfoResp], error) {
	pointsDao := model.NewPointsDao(plugin.MongoCli().GetDB())

	var startTime, endTime *time.Time

	if req.StartTime != "" {
		timeInt, err := strconv.ParseInt(req.StartTime, 10, 64)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr
		}
		t := time.Unix(timeInt, 0).UTC()
		startTime = &t
	}

	if req.EndTime != "" {
		timeInt, err := strconv.ParseInt(req.EndTime, 10, 64)
		if err != nil {
			return nil, freeErrors.ParameterInvalidErr
		}
		t := time.Unix(timeInt, 0).UTC()
		endTime = &t
	}

	total, list, err := pointsDao.QueryPointsRecordsWithUserInfo(
		ctx,
		orgID,
		req.Keyword,
		req.MinPoints,
		req.MaxPoints,
		req.PointsType,
		startTime,
		endTime,
		&req.DepPagination,
	)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 转换为响应格式
	respList := make([]*PointsRecordWithUserInfoResp, len(list))
	for i, item := range list {
		respList[i] = &PointsRecordWithUserInfoResp{
			ID:             item.Points.ID,
			Points:         item.Points.Points,
			ImServerUserId: item.Points.ImServerUserId,
			OrganizationId: item.Points.OrganizationId,
			PointsType:     item.Points.PointsType,
			Source:         item.Points.Source,
			Description:    item.Points.Description,
			CreatedAt:      item.Points.CreatedAt,
			UpdatedAt:      item.Points.UpdatedAt,
			UserInfo:       item.User,
			AttributeInfo:  item.Attribute,
		}
	}

	return &paginationUtils.ListResp[*PointsRecordWithUserInfoResp]{
		List:  respList,
		Total: total,
	}, nil
}

// ============ 用户端积分查询功能 ============

// UserPointsReq 用户端查询积分请求体
type UserPointsReq struct {
	paginationUtils.DepPagination `json:",inline"`
}

// UserPointsResp 用户端积分响应体
type UserPointsResp struct {
	TotalPoints   int64     `json:"total_points"`   // 用户总积分
	PointsRecords []*Points `json:"points_records"` // 积分记录列表
	Total         int64     `json:"total"`          // 记录总数
}

type Points struct {
	ID          primitive.ObjectID `json:"id"`
	Points      int64              `json:"points"`
	PointsType  model.PointsType   `json:"points_type"`
	Source      string             `json:"source,omitempty"`
	Description string             `json:"description,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
}

// QueryUserPoints 查询用户积分列表（用户端）
func (w *PointsSvc) QueryUserPoints(ctx context.Context, req *UserPointsReq, imServerUserId string, orgID primitive.ObjectID) (*UserPointsResp, error) {
	// 查询用户积分记录
	pointsDao := model.NewPointsDao(plugin.MongoCli().GetDB())
	total, list, err := pointsDao.QueryUserPointsRecords(ctx, imServerUserId, orgID, &req.DepPagination)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// 查询用户总积分
	orgUserDao := OrgModel.NewOrganizationUserDao(plugin.MongoCli().GetDB())
	orgUser, err := orgUserDao.GetByUserIMServerUserId(ctx, imServerUserId)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	var totalPoints int64 = 0
	if orgUser.Points != 0 {
		totalPoints = orgUser.Points
	}

	// 转换积分记录格式
	pointsRecords := make([]*Points, len(list))
	for i, item := range list {
		pointsRecords[i] = &Points{
			ID:          item.ID,
			Points:      item.Points,
			PointsType:  item.PointsType,
			Source:      item.Source,
			Description: item.Description,
			CreatedAt:   item.CreatedAt,
		}
	}

	return &UserPointsResp{
		TotalPoints:   totalPoints,
		PointsRecords: pointsRecords,
		Total:         total,
	}, nil
}
