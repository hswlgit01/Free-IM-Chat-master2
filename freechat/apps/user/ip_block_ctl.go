package user

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	adminModel "github.com/openimsdk/chat/pkg/common/db/model/admin"
	"github.com/openimsdk/tools/apiresp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type IPBlockItem struct {
	IP            string    `json:"ip"`
	LimitRegister bool      `json:"limit_register"`
	LimitLogin    bool      `json:"limit_login"`
	CreateTime    time.Time `json:"create_time"`
}

type IPBlockListResp struct {
	Total int64         `json:"total"`
	Data  []IPBlockItem `json:"data"`
}

type CreateIPBlockReq struct {
	IP            string `json:"ip" binding:"required"`
	LimitRegister bool   `json:"limit_register"`
	LimitLogin    bool   `json:"limit_login"`
}

type DeleteIPBlockReq struct {
	IPs []string `json:"ips" binding:"required"`
}

func normalizeManagedIP(ip string) (string, error) {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return "", freeErrors.ParameterInvalidErr
	}
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return "", freeErrors.ApiErr("IP地址格式不正确")
	}
	return parsed.String(), nil
}

// CmsGetIPBlockList 查询 IP 封锁列表。
func (w *UserCtl) CmsGetIPBlockList(c *gin.Context) {
	if _, err := middleware.GetOrgInfoFromCtx(c); err != nil {
		apiresp.GinError(c, err)
		return
	}

	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	state := int32(0)
	if raw := strings.TrimSpace(c.Query("state")); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			apiresp.GinError(c, freeErrors.ParameterInvalidErr)
			return
		}
		state = int32(v)
	}

	dao, err := adminModel.NewIPForbidden(plugin.MongoCli().GetDB())
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	total, rows, err := dao.Search(c, c.Query("keyword"), state, page)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}

	resp := IPBlockListResp{Total: total, Data: make([]IPBlockItem, 0, len(rows))}
	for _, row := range rows {
		resp.Data = append(resp.Data, IPBlockItem{
			IP:            row.IP,
			LimitRegister: row.LimitRegister,
			LimitLogin:    row.LimitLogin,
			CreateTime:    row.CreateTime,
		})
	}
	apiresp.GinSuccess(c, resp)
}

// CmsPostCreateIPBlock 新增或更新 IP 封锁配置。默认启用“限制注册”。
func (w *UserCtl) CmsPostCreateIPBlock(c *gin.Context) {
	if _, err := middleware.GetOrgInfoFromCtx(c); err != nil {
		apiresp.GinError(c, err)
		return
	}
	var req CreateIPBlockReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	ip, err := normalizeManagedIP(req.IP)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	if !req.LimitRegister && !req.LimitLogin {
		req.LimitRegister = true
	}

	_, err = plugin.MongoCli().GetDB().Collection("ip_forbidden").UpdateOne(
		c,
		bson.M{"ip": ip},
		bson.M{"$set": bson.M{
			"ip":             ip,
			"limit_register": req.LimitRegister,
			"limit_login":    req.LimitLogin,
			"create_time":    time.Now(),
		}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}

	apiresp.GinSuccess(c, map[string]any{})
}

// CmsPostDeleteIPBlock 删除 IP 封锁配置。
func (w *UserCtl) CmsPostDeleteIPBlock(c *gin.Context) {
	if _, err := middleware.GetOrgInfoFromCtx(c); err != nil {
		apiresp.GinError(c, err)
		return
	}
	var req DeleteIPBlockReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	ips := make([]string, 0, len(req.IPs))
	seen := make(map[string]struct{}, len(req.IPs))
	for _, raw := range req.IPs {
		ip, err := normalizeManagedIP(raw)
		if err != nil {
			apiresp.GinError(c, err)
			return
		}
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		ips = append(ips, ip)
	}
	if len(ips) == 0 {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	_, err := plugin.MongoCli().GetDB().Collection("ip_forbidden").DeleteMany(c, bson.M{"ip": bson.M{"$in": ips}})
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}

	apiresp.GinSuccess(c, map[string]any{})
}
