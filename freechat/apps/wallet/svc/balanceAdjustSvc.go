package svc

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	opModel "github.com/openimsdk/chat/freechat/apps/operationLog/model"
	opSvc "github.com/openimsdk/chat/freechat/apps/operationLog/svc"
	orgModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	walletTsModel "github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 后台手动调节用户可用余额。
//
// 这是运营的补救手段（补发、纠错、人工返还等），直接改动用户的钱，
// 所以设计上把重点放在「可追溯」而不是「流程管控」：
//
//   - 不做审批流。手动调节的价值就在于及时；加审批会让它失去意义。
//     真正的约束靠事后可查：每一笔都落用户账单 + 后台操作日志，谁做的、
//     给谁、多少、为什么，全部留痕。若客户后续需要审批，可基于操作日志再加一层。
//   - 只允许 SuperAdmin / BackendAdmin 操作。GroupManager（管理员）和
//     TermManager（团队长）是业务角色，不该直接碰钱。
//   - 必须填写调整原因，且会写进用户账单的备注里 —— 用户能在账单看到
//     「后台调整」及原因，而不是余额莫名其妙变了。
//   - 幂等：调用方传 requestID，重复提交（网络重试、连点两下）只生效一次。
//   - 单笔封顶，防手滑多打一个 0。

// AdjustBalanceRequest 后台调节余额的入参。
type AdjustBalanceRequest struct {
	OrgID          primitive.ObjectID // 组织
	OperatorUserID string             // 操作人 organization_user.user_id
	OperatorImID   string             // 操作人 IM ID，仅用于操作日志
	TargetUserID   string             // 被调整用户 organization_user.user_id
	CurrencyID     primitive.ObjectID // 币种
	Amount         decimal.Decimal    // 调整金额，正数增加、负数扣减
	Reason         string             // 调整原因，必填
	RequestID      string             // 幂等键，同一个只生效一次
}

// maxSingleAdjust 单笔调节上限（绝对值）。
//
// 纯粹是防手滑（多打一个 0）的保险丝，不是业务规则。默认 100000，
// 可用 WALLET_MAX_SINGLE_ADJUST 覆盖。这个默认值是拍的，
// 客户如果有明确的额度规定，应当按其规定调整。
func maxSingleAdjust() decimal.Decimal {
	if v := strings.TrimSpace(os.Getenv("WALLET_MAX_SINGLE_ADJUST")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return decimal.NewFromFloat(f)
		}
	}
	return decimal.NewFromInt(100000)
}

type BalanceAdjustSvc struct{}

func NewBalanceAdjustSvc() *BalanceAdjustSvc { return &BalanceAdjustSvc{} }

// AdjustUserBalance 调整指定用户的可用余额。
//
// 返回调整后的可用余额。
func (s *BalanceAdjustSvc) AdjustUserBalance(ctx context.Context, req *AdjustBalanceRequest) (string, error) {
	// ---- 入参校验 ----
	if strings.TrimSpace(req.Reason) == "" {
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, "调整原因不能为空")
	}
	if strings.TrimSpace(req.RequestID) == "" {
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, "缺少幂等键 requestId")
	}
	if req.Amount.IsZero() {
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, "调整金额不能为 0")
	}
	if limit := maxSingleAdjust(); req.Amount.Abs().GreaterThan(limit) {
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams,
			"单笔调整金额不能超过 "+limit.String())
	}

	db := plugin.MongoCli().GetDB()

	// ---- 被调整用户必须属于本组织 ----
	// 防止跨组织越权：后台账号只能调本组织用户的余额。
	orgUserDao := orgModel.NewOrganizationUserDao(db)
	targetOrgUser, err := orgUserDao.GetByUserIdAndOrgID(ctx, req.TargetUserID, req.OrgID.Hex())
	if err != nil || targetOrgUser == nil {
		log.ZWarn(ctx, "调整余额：目标用户不属于该组织", err,
			"target_user_id", req.TargetUserID, "org_id", req.OrgID.Hex())
		return "", errs.NewCodeError(freeErrors.ErrInvalidParams, "该用户不属于当前组织")
	}

	// ---- 钱包 ----
	walletInfoDao := walletModel.NewWalletInfoDao(db)
	walletInfo, err := walletInfoDao.GetByOwnerIdAndOwnerType(ctx, req.TargetUserID, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil || walletInfo == nil {
		return "", errs.NewCodeError(freeErrors.WalletNotOpenCode, freeErrors.ErrorMessages[freeErrors.WalletNotOpenCode])
	}

	// ---- 幂等 ----
	// 用 request_id 上的唯一索引兜底：并发重复提交时只有一条能插入成功。
	// 先查一次是为了给重复提交返回友好结果，而不是报唯一键冲突。
	if done, balance := s.findDoneAdjust(ctx, db, req.RequestID); done {
		log.ZInfo(ctx, "调整余额：幂等命中，跳过重复执行",
			"request_id", req.RequestID, "target_user_id", req.TargetUserID)
		return balance, nil
	}

	// ---- 执行 ----
	balanceDao := walletModel.NewWalletBalanceDao(db)
	var newBalance string

	session, err := db.Client().StartSession()
	if err != nil {
		return "", errs.Wrap(err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// 幂等标记先插入：唯一索引保证并发下只有一个事务能过
		if _, insErr := db.Collection(adjustLogCollection).InsertOne(sessCtx, bson.M{
			"request_id":       req.RequestID,
			"org_id":           req.OrgID,
			"operator_user_id": req.OperatorUserID,
			"target_user_id":   req.TargetUserID,
			"currency_id":      req.CurrencyID,
			"amount":           req.Amount.String(),
			"reason":           req.Reason,
			"created_at":       time.Now().UTC(),
		}); insErr != nil {
			if mongo.IsDuplicateKeyError(insErr) {
				return nil, errDuplicateAdjust
			}
			return nil, insErr
		}

		// 改余额 + 记账单流水（同一函数内完成，扣减时带
		// available_balance >= |amount| 的原子条件，余额不足会直接失败并回滚）
		if err := balanceDao.UpdateAvailableBalanceAndAddTsRecord(
			sessCtx,
			walletInfo.ID,
			req.CurrencyID,
			req.Amount,
			walletTsModel.TsRecordTypeAdminAdjust,
			req.OperatorUserID, // source：记录是谁操作的
			req.Reason,         // remark：用户在账单里能看到原因
		); err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		if err == errDuplicateAdjust {
			if done, balance := s.findDoneAdjust(ctx, db, req.RequestID); done {
				return balance, nil
			}
			return "", errs.NewCodeError(freeErrors.ErrSystem, "该请求正在处理中，请勿重复提交")
		}
		log.ZError(ctx, "调整余额失败", err,
			"target_user_id", req.TargetUserID, "amount", req.Amount.String())
		return "", err
	}

	// ---- 读回最新余额（仅用于返回展示，失败不影响调整结果）----
	if bal, e := balanceDao.GetByWalletIdAndCurrencyId(ctx, walletInfo.ID, req.CurrencyID); e == nil && bal != nil {
		newBalance = bal.AvailableBalance.String()
	}

	// ---- 操作日志（在事务外，失败只告警：钱已经调好了，不能因为日志失败回滚）----
	if logErr := opSvc.NewOperationLogSvc().InternalCreateOperationLog(ctx, &opSvc.InternalCreateOperationLogReq{
		OrgId:          req.OrgID,
		UserId:         req.OperatorUserID,
		ImServerUserId: req.OperatorImID,
		OperationType:  opModel.OpTypeAdjustUserBalance,
		Details: map[string]interface{}{
			"target_user_id":     req.TargetUserID,
			"target_im_user_id":  targetOrgUser.ImServerUserId,
			"currency_id":        req.CurrencyID.Hex(),
			"amount":             req.Amount.String(),
			"reason":             req.Reason,
			"request_id":         req.RequestID,
			"balance_after":      newBalance,
		},
	}); logErr != nil {
		log.ZError(ctx, "调整余额：写操作日志失败（余额已调整成功）", logErr,
			"request_id", req.RequestID, "target_user_id", req.TargetUserID)
	}

	log.ZInfo(ctx, "后台调整用户余额成功",
		"operator", req.OperatorUserID, "target", req.TargetUserID,
		"amount", req.Amount.String(), "balance_after", newBalance, "reason", req.Reason)
	return newBalance, nil
}

const adjustLogCollection = "wallet_admin_adjust"

var errDuplicateAdjust = errs.New("duplicate adjust request")

// findDoneAdjust 查询该幂等键是否已执行过。返回是否已执行、以及当前余额。
func (s *BalanceAdjustSvc) findDoneAdjust(ctx context.Context, db *mongo.Database, requestID string) (bool, string) {
	var rec struct {
		TargetUserID string             `bson:"target_user_id"`
		CurrencyID   primitive.ObjectID `bson:"currency_id"`
	}
	if err := db.Collection(adjustLogCollection).FindOne(ctx, bson.M{"request_id": requestID}).Decode(&rec); err != nil {
		return false, ""
	}
	walletInfo, err := walletModel.NewWalletInfoDao(db).
		GetByOwnerIdAndOwnerType(ctx, rec.TargetUserID, walletModel.WalletInfoOwnerTypeOrdinary)
	if err != nil || walletInfo == nil {
		return true, ""
	}
	bal, err := walletModel.NewWalletBalanceDao(db).GetByWalletIdAndCurrencyId(ctx, walletInfo.ID, rec.CurrencyID)
	if err != nil || bal == nil {
		return true, ""
	}
	return true, bal.AvailableBalance.String()
}

// EnsureAdjustIndex 建幂等键的唯一索引。随其它索引一起在启动时创建。
func EnsureAdjustIndex(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(adjustLogCollection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "request_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}
