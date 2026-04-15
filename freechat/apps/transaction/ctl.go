package transaction

import (
	"encoding/json"
	"fmt"
	"os"

	"time"

	"github.com/openimsdk/chat/freechat/middleware"

	"github.com/openimsdk/chat/freechat/apps/notification/svc"
	"github.com/openimsdk/chat/freechat/apps/transaction/dto"
	svc2 "github.com/openimsdk/chat/freechat/apps/transaction/svc"

	"github.com/openimsdk/tools/log"

	"github.com/openimsdk/chat/pkg/common/mctx"

	"github.com/openimsdk/chat/freechat/constant"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
)

// TransactionCtl 交易控制器，处理HTTP请求
type TransactionCtl struct{}

func NewTransactionCtl() *TransactionCtl {
	return &TransactionCtl{}
}

// CreateTransaction 创建交易
func (ctl *TransactionCtl) CreateTransaction(c *gin.Context) {
	var req dto.CreateTransactionReq
	if err := c.ShouldBindJSON(&req); err != nil {
		log.ZError(c, "bind json failed", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}
	// 从上下文获取用户ID
	req.SenderID = mctx.GetOpUserID(c)
	orgID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
	}
	req.OrgID = orgID
	// 调用服务层
	transactionService := svc2.NewTransactionService()
	transactionID, err := transactionService.CreateTransaction(c, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, gin.H{"transaction_id": transactionID})
}

// ReceiveTransaction 领取交易
func (ctl *TransactionCtl) ReceiveTransaction(c *gin.Context) {
	var req dto.ReceiveTransactionReq
	if err := c.ShouldBindJSON(&req); err != nil {
		log.ZError(c, "bind json failed in ReceiveTransaction", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}
	// 从上下文获取用户ID
	req.ReceiverID = mctx.GetOpUserID(c)
	orgID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
	}
	req.OrgID = orgID
	// 调用服务层
	transactionService := svc2.NewTransactionService()
	amount, err := transactionService.ReceiveTransaction(c, &req)
	if err != nil {
		log.ZError(c, "receive transaction failed", err)
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, gin.H{"amount": amount})
}

// ReceiveTransactionStress 压测专用：从 body 读取 receiver_id 调用领取，需开启 STRESS_TEST_RECEIVE=1 且可选 X-Stress-Test-Secret
func (ctl *TransactionCtl) ReceiveTransactionStress(c *gin.Context) {
	if os.Getenv("STRESS_TEST_RECEIVE") != "1" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrForbidden, "stress test not enabled"))
		return
	}
	if secret := os.Getenv("STRESS_TEST_SECRET"); secret != "" {
		if c.GetHeader("X-Stress-Test-Secret") != secret {
			apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrForbidden, "invalid secret"))
			return
		}
	}
	var req dto.ReceiveTransactionReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}
	if req.ReceiverID == "" || req.TransactionID == "" || req.OrgID == "" {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, "receiver_id, transaction_id, org_id required"))
		return
	}
	// 压测接口统一标记为「跳过群校验」，单次验证与并发压测走完全相同的业务逻辑
	ctx := svc2.WithStressSkipGroupCheck(c.Request.Context())
	transactionService := svc2.NewTransactionService()
	amount, err := transactionService.ReceiveTransaction(ctx, &req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, gin.H{"amount": amount})
}

// CheckUserReceived 查询用户是否接收了交易
func (ctl *TransactionCtl) CheckUserReceived(c *gin.Context) {
	var req dto.CheckUserReceivedReq
	if err := c.ShouldBindJSON(&req); err != nil {
		log.ZError(c, "bind json failed in CheckUserReceived", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}
	req.UserID = mctx.GetOpUserID(c)
	// 查询接收状态
	transactionService := svc2.NewTransactionService()
	resp, err := transactionService.CheckUserReceived(c, &req)
	if err != nil {
		log.ZError(c, "check user received failed", err)
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// GetTransactionReceiveDetails 查询交易接收详情
func (ctl *TransactionCtl) GetTransactionReceiveDetails(c *gin.Context) {
	var req dto.QueryTransactionReceiveDetailsReq
	if err := c.ShouldBindQuery(&req); err != nil {
		log.ZError(c, "bind query failed in GetTransactionReceiveDetails", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}
	// 当 token 未带操作人 ID 时，用客户端传的 IM 用户 ID 查领取记录，避免重装/缓存后显示 0.00
	if imID := c.GetHeader("X-User-IM-ID"); imID != "" {
		req.OpUserImID = imID
	}
	// 查询接收详情
	transactionService := svc2.NewTransactionService()
	resp, err := transactionService.GetTransactionReceiveDetails(c, &req)
	if err != nil {
		log.ZError(c, "get transaction receive details failed", err)
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, resp)
}

// GetUserReceiveHistory 查询用户24小时内接收/发起完成的交易记录
func (ctl *TransactionCtl) GetUserReceiveHistory(c *gin.Context) {
	var req dto.QueryUserReceiveHistoryReq
	if err := c.ShouldBindQuery(&req); err != nil {
		log.ZError(c, "bind query failed in GetUserReceiveHistory", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}

	// 从上下文获取用户ID
	req.UserID = mctx.GetOpUserID(c)
	orgID, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
	}
	req.OrgID = orgID
	// 查询用户接收记录
	transactionService := svc2.NewTransactionService()
	resp, err := transactionService.GetUserReceiveHistoryLast24Hours(c, &req)
	if err != nil {
		log.ZError(c, "get user receive history failed", err)
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// CheckTransactionCompleted 根据交易ID判断交易是否已领取完毕
func (ctl *TransactionCtl) CheckTransactionCompleted(c *gin.Context) {
	var req dto.CheckTransactionCompletedReq
	if err := c.ShouldBindJSON(&req); err != nil {
		log.ZError(c, "bind json failed in CheckTransactionCompleted", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}

	// 从上下文获取用户ID
	req.UserID = mctx.GetOpUserID(c)

	// 调用服务层
	transactionService := svc2.NewTransactionService()
	completed, received, err := transactionService.CheckTransactionCompleted(c, &req)
	if err != nil {
		log.ZError(c, "check transaction completed failed", err)
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, gin.H{
		"completed": completed,
		"received":  received,
	})
}

// TestSendNotification 测试发送通知
func (ctl *TransactionCtl) TestSendNotification(c *gin.Context) {
	var req struct {
		UserID string `json:"user_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		log.ZError(c, "bind json failed in TestSendNotification", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}

	// 获取通知服务
	notificationService := svc.NewNotificationService()

	// 构建测试通知内容
	content := fmt.Sprintf("这是一条测试通知消息，发送时间：%s", time.Now().Format("2006-01-02 15:04:05"))

	// 序列化通知内容
	textContent := map[string]any{
		"content": content,
	}

	// 构建额外数据
	extraData := map[string]interface{}{
		"test_id":   fmt.Sprintf("test-%d", time.Now().Unix()),
		"test_type": "notification_test",
		"send_time": time.Now().Format(time.RFC3339),
	}

	// 序列化额外数据
	extraDataJSON, err := json.Marshal(extraData)
	if err != nil {
		log.ZError(c, "marshal extra data failed", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, "序列化额外数据失败"))
		return
	}

	// 构建离线推送信息
	offlinePushInfo := &sdkws.OfflinePushInfo{
		Title:         "Payment Information",
		Desc:          content,
		IOSPushSound:  "default",
		IOSBadgeCount: true,
	}

	// 当前时间
	nowUTC := time.Now().UTC()

	msgData := svc.SendMsg{
		SendID:           constant.NOTIFICATION_ADMIN_PAYMENT_SEND_ID, // 系统发送者ID 001
		RecvID:           req.UserID,                                  // 接收者ID
		ContentType:      constantpb.Text,                             // 文本消息类型
		SessionType:      constantpb.SingleChatType,
		Content:          textContent,
		SenderPlatformID: 10,
		SendTime:         nowUTC.UnixMilli(),
		Ex:               string(extraDataJSON),
		OfflinePushInfo:  offlinePushInfo,
	}

	// 发送通知
	if err := notificationService.SendNotification(c, msgData, constant.NOTIFICATION_ADMIN_PAYMENT_SEND_ID); err != nil {
		log.ZError(c, "send notification failed", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, fmt.Sprintf("发送通知失败: %v", err)))
		return
	}

	apiresp.GinSuccess(c, gin.H{
		"message": "测试通知发送成功",
		"content": content,
		"user_id": req.UserID,
	})
}

func (ctl *TransactionCtl) GetOrgTransactionList(c *gin.Context) {
	org, err := mctx.GetOrgId(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	var req dto.ListOrgTransactions
	if err := c.ShouldBindJSON(&req); err != nil {
		log.ZError(c, "bind json failed int GetOrgTransactionList", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}
	req.OrgID = org

	transactionService := svc2.NewTransactionService()
	resp, err := transactionService.GetOrgTransactionList(c, &req)
	apiresp.GinSuccess(c, resp)
}

// QueryTransactionRecords 查询交易记录
func (ctl *TransactionCtl) QueryTransactionRecords(c *gin.Context) {
	var req dto.TransactionRecordQueryReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	// 获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	transactionSvc := svc2.NewTransactionService()
	resp, err := transactionSvc.QueryTransactionRecords(c.Request.Context(), &req, org.ID.Hex())
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// 查询用户领取详情
func (ctl *TransactionCtl) QueryReceiveRecords(c *gin.Context) {
	var req dto.ReceiveRecordQueryReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}

	// 获取组织信息
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	transactionSvc := svc2.NewTransactionService()
	resp, err := transactionSvc.QueryReceiveRecords(c.Request.Context(), &req, org.ID.Hex())
	if err != nil {
		apiresp.GinError(c, err)
		return
	}

	apiresp.GinSuccess(c, resp)
}

// RepairTransactionConsistency 修复红包交易数据一致性
// 当Redis与MongoDB数据不一致时调用此接口进行修复
func (ctl *TransactionCtl) RepairTransactionConsistency(c *gin.Context) {
	var req struct {
		TransactionID string `json:"transaction_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		log.ZError(c, "bind json failed in RepairTransactionConsistency", err)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams]))
		return
	}

	transactionSvc := svc2.NewTransactionService()
	result, err := transactionSvc.RepairTransactionConsistency(c.Request.Context(), req.TransactionID)
	if err != nil {
		log.ZError(c, "repair transaction consistency failed", err, "transaction_id", req.TransactionID)
		apiresp.GinError(c, errs.NewCodeError(freeErrors.ErrSystem, err.Error()))
		return
	}

	apiresp.GinSuccess(c, result)
}
