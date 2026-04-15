package transaction

import (
	"context"
	"encoding/json"
	"fmt"
	openImModel "github.com/openimsdk/chat/freechat/third/openIm/model"
	"strconv"
	"sync"
	"time"

	notificationSvc "github.com/openimsdk/chat/freechat/apps/notification/svc"
	"github.com/openimsdk/chat/freechat/apps/transaction/model"
	"github.com/openimsdk/chat/freechat/apps/transaction/svc"
	walletModel "github.com/openimsdk/chat/freechat/apps/wallet/model"
	walletTransactionRecordModel "github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"

	"github.com/google/uuid"
	depconstant "github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	constantpb "github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// TransactionCronJob 交易定时任务
type TransactionCronJob struct {
	cron               *cron.Cron
	transactionService *svc.TransactionService
	isRunning          sync.Mutex // 防止定时任务重叠执行
}

// NewTransactionCronJob 创建交易定时任务
func NewTransactionCronJob() *TransactionCronJob {
	c := cron.New(cron.WithSeconds())
	return &TransactionCronJob{
		cron:               c,
		transactionService: svc.NewTransactionService(),
	}
}

// Start 启动定时任务
func (j *TransactionCronJob) Start() {
	// 每分钟执行一次定时任务
	_, err := j.cron.AddFunc("0 * * * * *", j.processExpiredTransactions)
	if err != nil {
		log.ZError(context.Background(), "添加定时任务失败", err)
		return
	}
	j.cron.Start()
	log.ZInfo(context.Background(), "交易定时任务已启动")
}

// Stop 停止定时任务
func (j *TransactionCronJob) Stop() {
	j.cron.Stop()
	log.ZInfo(context.Background(), "交易定时任务已停止")
}

// processExpiredTransactions 处理过期交易
func (j *TransactionCronJob) processExpiredTransactions() {
	// 尝试获取锁，如果获取不到说明上次还在执行
	if !j.isRunning.TryLock() {
		log.ZWarn(context.Background(), "上次定时任务仍在执行中，跳过本次执行", nil)
		return
	}
	defer j.isRunning.Unlock()

	// 使用新的ProcessExpiredTransactions方法处理过期交易
	ctx := context.Background()
	if err := j.ProcessExpiredTransactions(ctx); err != nil {
		log.ZError(ctx, "处理过期交易失败", err)
	}
}

// handleExpiredTransaction 处理单个过期交易
func (j *TransactionCronJob) handleExpiredTransaction(ctx context.Context, transaction *model.Transaction) {
	// 创建统一的UTC时间引用，所有时间操作都使用这个变量
	nowUTC := time.Now().UTC()

	// 为每个交易添加分布式锁
	transactionLockKey := fmt.Sprintf("%s%s", depconstant.TransactionLockPrefix, transaction.TransactionID)
	redisCli := plugin.RedisCli()

	// 生成锁值，包含节点信息和时间戳
	lockValue := fmt.Sprintf("node-%d-%d", nowUTC.UnixNano()%1000000, nowUTC.UnixNano())

	// 设置锁的过期时间为30秒，防止死锁
	ok, err := redisCli.SetNX(ctx, transactionLockKey, lockValue, 30*time.Second).Result()
	if err != nil {
		log.ZError(ctx, "获取交易处理锁失败", err, "transaction_id", transaction.TransactionID)
		return
	}
	if !ok {
		// 查看当前锁的持有者
		currentLockValue, _ := redisCli.Get(ctx, transactionLockKey).Result()
		log.ZInfo(ctx, "该交易正在被其他实例处理",
			"transaction_id", transaction.TransactionID,
			"current_lock_holder", currentLockValue)
		return
	}

	// 使用defer确保锁被正确释放
	defer func() {
		// 只有持有锁的节点才能释放锁
		const unlockScript = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end`

		result, err := redisCli.Eval(ctx, unlockScript, []string{transactionLockKey}, lockValue).Result()
		if err != nil {
			log.ZWarn(ctx, "释放交易锁失败", err, "transaction_id", transaction.TransactionID)
		} else if result.(int64) == 0 {
			log.ZWarn(ctx, "锁已被其他节点持有，无法释放", nil, "transaction_id", transaction.TransactionID)
		}
	}()

	log.ZInfo(ctx, "开始处理过期交易", "transaction_id", transaction.TransactionID, "created_at", transaction.CreatedAt.Format(time.RFC3339))

	// 从Redis获取剩余金额和剩余个数
	redisTransactionKey := fmt.Sprintf("%s%s", depconstant.TransactionKeyPrefix, transaction.TransactionID)
	redisReceiversKey := fmt.Sprintf("%s%s", depconstant.TransactionReceiversPrefix, transaction.TransactionID)

	var remainingAmount, remainingCount string

	remainingAmount, err = redisCli.HGet(ctx, redisTransactionKey, "remaining_amount").Result()
	if err != nil {
		if err == redis.Nil {
			// Redis中不存在该交易信息
			// 根据交易ID去表中查询已经领取过的，然后计算退款金额以及个数，执行退款
			log.ZInfo(ctx, "Redis中不存在该交易信息，从数据库计算退款", "transaction_id", transaction.TransactionID)

			// 从数据库计算剩余金额和数量
			calculatedAmount, calculatedCount, err := j.transactionService.CalculateRemainingAmountFromDB(ctx, transaction)
			if err != nil {
				log.ZError(ctx, "从数据库计算退款金额失败", err, "transaction_id", transaction.TransactionID)
				return
			}

			// 如果没有剩余金额可退款，跳过处理
			if calculatedAmount == "" || calculatedAmount == "0" {
				log.ZInfo(ctx, "无剩余金额需要退款", "transaction_id", transaction.TransactionID)
				return
			}

			// 使用计算出的金额继续处理退款逻辑
			remainingAmount = calculatedAmount
			remainingCount = calculatedCount
			log.ZInfo(ctx, "从数据库计算得到退款信息",
				"transaction_id", transaction.TransactionID,
				"remaining_amount", remainingAmount,
				"remaining_count", remainingCount)
		} else {
			// 其他错误
			log.ZError(ctx, "获取Redis交易剩余金额失败", err, "transaction_id", transaction.TransactionID)
			return
		}
	} else {
		// Redis中有数据但金额为空，也跳过处理
		if remainingAmount == "" {
			return
		}

		// 获取剩余个数
		remainingCount, err = redisCli.HGet(ctx, redisTransactionKey, "remaining_count").Result()
		if err != nil && err != redis.Nil {
			log.ZError(ctx, "获取Redis交易剩余个数失败", err, "transaction_id", transaction.TransactionID)
			return
		}

		// 如果剩余个数为空，设置默认值
		if remainingCount == "" {
			remainingCount = "0"
		}
	}

	// 记录关键信息日志 - 交易ID和剩余金额
	log.ZInfo(ctx, "处理过期交易", "transaction_id", transaction.TransactionID, "remaining_amount", remainingAmount)

	// 使用Redis中的剩余金额和剩余个数
	amount := remainingAmount

	// 2. 开启MongoDB事务
	mongoCli := plugin.MongoCli().GetDB()
	session, err := mongoCli.Client().StartSession()
	if err != nil {
		log.ZError(ctx, "创建MongoDB事务失败", err, "transaction_id", transaction.TransactionID)
		return
	}
	defer session.EndSession(ctx)

	// 在事务中执行操作
	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// 在事务内再次检查交易状态，防止并发处理
		transactionDao := model.NewTransactionDao(plugin.MongoCli().GetDB())

		// 重新获取最新的交易状态
		currentTransaction, err := transactionDao.GetByTransactionID(sessCtx, transaction.TransactionID)
		if err != nil {
			log.ZError(ctx, "获取交易信息失败", err, "transaction_id", transaction.TransactionID)
			return nil, err
		}

		// 如果交易状态不是待领取，则跳过处理
		if currentTransaction.Status != model.TransactionStatusPending {
			log.ZInfo(ctx, "交易状态已变更，跳过处理", "transaction_id", transaction.TransactionID,
				"current_status", currentTransaction.Status, "expected_status", model.TransactionStatusPending)
			return nil, nil
		}

		// 转换剩余金额字符串为Decimal128
		var remainingAmountDecimal128 primitive.Decimal128
		var err2 error
		if amount != "" && amount != "0" {
			remainingAmountDecimal128, err2 = primitive.ParseDecimal128(amount)
			if err2 != nil {
				log.ZError(ctx, "金额转换失败", err2, "amount", amount)
				remainingAmountDecimal128, _ = primitive.ParseDecimal128("0")
			}
		} else {
			remainingAmountDecimal128, _ = primitive.ParseDecimal128("0")
		}
		atoi, _ := strconv.Atoi(remainingCount)
		// 使用新方法同时更新状态、剩余金额和剩余数量
		if err := transactionDao.UpdateTransactionExpired(sessCtx, transaction.TransactionID, remainingAmountDecimal128, atoi); err != nil {
			log.ZError(ctx, "更新交易状态失败", err, "transaction_id", transaction.TransactionID)
			return nil, err
		}

		// 如果是有效金额，退回给发起者
		if amount != "" && amount != "0" {
			// 调用余额服务，将剩余金额退回给发起者
			amountDecimal, err := decimal.NewFromString(amount)
			if err != nil {
				log.ZError(ctx, "金额格式转换失败", err, "amount", amount)
				return nil, err
			}

			walletDao := walletModel.NewWalletBalanceDao(plugin.MongoCli().GetDB())

			// 根据交易类型决定处理方式
			if transaction.TransactionType == model.TransactionTypeNormalPacket ||
				transaction.TransactionType == model.TransactionTypeLuckyPacket ||
				transaction.TransactionType == model.TransactionTypeP2PRedPacket ||
				transaction.TransactionType == model.TransactionTypeGroupExclusive ||
				transaction.TransactionType == model.TransactionTypePasswordPacket {
				// 减少红包冻结余额
				if err := walletDao.UpdateRedPacketFrozenBalance(sessCtx, transaction.WalletID, transaction.CurrencyId, amountDecimal.Neg()); err != nil {
					log.ZError(ctx, "减少红包冻结余额失败", err, "transaction_id", transaction.TransactionID, "sender_id", transaction.SenderID, "amount", amount)
					return nil, err
				}

				// 增加可用余额并记录交易
				if err := walletDao.UpdateAvailableBalanceAndAddTsRecord(
					sessCtx,
					transaction.WalletID,
					transaction.CurrencyId,
					amountDecimal,
					walletTransactionRecordModel.TsRecordTypeRedPacketRefund,
					"",
					transaction.Greeting); err != nil {
					log.ZError(ctx, "红包退款失败", err, "transaction_id", transaction.TransactionID, "sender_id", transaction.SenderID, "amount", amount)
					return nil, err
				}
			} else if transaction.TransactionType == model.TransactionTypeTransfer {
				// 减少转账冻结余额
				if err := walletDao.UpdateTransferFrozenBalance(sessCtx, transaction.WalletID, transaction.CurrencyId, amountDecimal.Neg()); err != nil {
					log.ZError(ctx, "减少转账冻结余额失败", err, "transaction_id", transaction.TransactionID, "sender_id", transaction.SenderID, "amount", amount)
					return nil, err
				}

				// 增加可用余额并记录交易
				if err := walletDao.UpdateAvailableBalanceAndAddTsRecord(
					sessCtx,
					transaction.WalletID,
					transaction.CurrencyId,
					amountDecimal,
					walletTransactionRecordModel.TsRecordTypeTransferRefund,
					"",
					transaction.Greeting); err != nil {
					log.ZError(ctx, "转账退款失败", err, "transaction_id", transaction.TransactionID, "sender_id", transaction.SenderID, "amount", amount)
					return nil, err
				}
			} else {
				log.ZError(ctx, "无效的交易类型", nil, "transaction_type", transaction.TransactionType)
				return nil, errs.NewCodeError(freeErrors.ErrInvalidParams, freeErrors.ErrorMessages[freeErrors.ErrInvalidParams])
			}

			// 创建退款记录
			refundID := uuid.New().String()
			refundDao := model.NewRefundRecordDao(plugin.MongoCli().GetDB())
			refundRecord := &model.RefundRecord{
				RefundID:        refundID,
				TransactionID:   transaction.TransactionID,
				UserID:          transaction.SenderID,
				UserImID:        transaction.SenderImID,
				TransactionType: transaction.TransactionType,
				RefundAmount:    remainingAmountDecimal128,
				RefundCount:     atoi,
				RefundReason:    model.RefundReasonExpired,
				WalletID:        transaction.WalletID,
				CurrencyID:      transaction.CurrencyId,
				OrgID:           transaction.OrgID,
				RefundTime:      nowUTC,
				Remark:          fmt.Sprintf("交易过期自动退款，原交易备注：%s", transaction.Greeting),
			}

			if err := refundDao.Create(sessCtx, refundRecord); err != nil {
				log.ZError(ctx, "创建退款记录失败", err, "transaction_id", transaction.TransactionID, "refund_id", refundID)
				return nil, err
			}

			log.ZInfo(ctx, "已退还剩余金额", "transaction_id", transaction.TransactionID, "sender_id", transaction.SenderID, "amount", amount, "remaining_count", remainingCount, "refund_id", refundID, "time", nowUTC.Format(time.RFC3339))
		}

		return nil, nil
	})

	if err != nil {
		// 如果事务失败，检查交易状态是否已经被其他进程更新
		checkDao := model.NewTransactionDao(plugin.MongoCli().GetDB())
		finalTransaction, checkErr := checkDao.GetByTransactionID(ctx, transaction.TransactionID)
		if checkErr == nil && finalTransaction.Status != model.TransactionStatusPending {
			log.ZInfo(ctx, "事务失败但交易已被其他进程处理",
				"transaction_id", transaction.TransactionID,
				"current_status", finalTransaction.Status)
			return // 其他进程已处理，不需要重试
		}

		log.ZError(ctx, "处理过期交易事务失败", err, "transaction_id", transaction.TransactionID)
		return
	}

	// 3. 删除Redis中的相关数据
	if err := redisCli.Del(ctx, redisTransactionKey, redisReceiversKey).Err(); err != nil {
		log.ZWarn(ctx, "删除Redis交易数据失败", err, "transaction_id", transaction.TransactionID)
	}

	// 4. 发送系统通知
	j.sendExpiredTransactionNotification(ctx, transaction, amount, remainingCount, nowUTC)

	log.ZInfo(ctx, "处理过期交易完成", "transaction_id", transaction.TransactionID, "time", nowUTC.Format(time.RFC3339))
}

// sendExpiredTransactionNotification 发送过期交易通知
func (j *TransactionCronJob) sendExpiredTransactionNotification(ctx context.Context, transaction *model.Transaction, refundAmount string, remainingCount string, nowUTC time.Time) {
	// 获取通知服务
	notificationService := notificationSvc.NewNotificationService()

	// 从数据库查询组织信息和币种信息
	db := plugin.MongoCli().GetDB()

	// 默认值
	notificationName := depconstant.DefaultNotificationName
	notificationFaceURL := depconstant.DefaultNotificationFaceURL

	// 查询通知账户信息
	userDao := openImModel.NewUserDao(db)
	user, err := userDao.Take(ctx, depconstant.NOTIFICATION_ADMIN_PAYMENT_SEND_ID)
	if err == nil {
		notificationName = user.Nickname
		notificationFaceURL = user.FaceURL
	}

	// 查询币种信息
	walletCurrencyDao := walletModel.NewWalletCurrencyDao(db)
	currency, err := walletCurrencyDao.GetById(ctx, transaction.CurrencyId)
	// 构建通知内容
	content := fmt.Sprintf("您的%s已过期，剩余金额%s已退回您的账户。",
		getTransactionTypeText(transaction.TransactionType), refundAmount)
	refundType := 11
	if transaction.TransactionType == model.TransactionTypeTransfer {
		refundType = 2
	}

	// 序列化通知内容
	textContent := map[string]any{
		"notificationName":    notificationName,
		"notificationFaceURL": notificationFaceURL,
		"notificationType":    500,
		"text":                "[退款]",
		"externalUrl":         "",
		"mixType":             1,
		"refundElem": map[string]any{
			"currency":   currency.Name,
			"amount":     refundAmount,
			"refundID":   transaction.TransactionID,
			"refundTime": nowUTC.Unix(),
			"refundType": refundType,
			"rate":       currency.ExchangeRate,
			"refundDesc": "",
		},
		"ex": "",
	}

	// 构建额外数据
	extraData := map[string]interface{}{
		"transaction_id":   transaction.TransactionID,
		"transaction_type": transaction.TransactionType,
		"refund_amount":    refundAmount,
		"remaining_count":  remainingCount,
	}

	// 序列化额外数据
	extraDataJSON, err := json.Marshal(extraData)
	if err != nil {
		log.ZWarn(ctx, "序列化额外数据失败", err,
			"transaction_id", transaction.TransactionID,
			"sender_id", transaction.SenderID)
	}

	// 构建离线推送信息
	offlinePushInfo := &sdkws.OfflinePushInfo{
		Title:         "交易通知",
		Desc:          content,
		IOSPushSound:  "default",
		IOSBadgeCount: true,
	}

	// 构建消息数据
	msgData := notificationSvc.SendMsg{
		SendID:          depconstant.NOTIFICATION_ADMIN_PAYMENT_SEND_ID, // 系统发送者ID
		RecvID:          transaction.SenderImID,
		SenderNickname:  notificationName,
		SenderFaceURL:   notificationFaceURL,
		ContentType:     constantpb.OANotification,
		SessionType:     constantpb.SingleChatType,
		Content:         textContent,
		SendTime:        nowUTC.UnixMilli(),
		Ex:              string(extraDataJSON),
		OfflinePushInfo: offlinePushInfo,
	}

	// 发送通知
	if err := notificationService.SendNotification(ctx, msgData, depconstant.NOTIFICATION_ADMIN_PAYMENT_SEND_ID); err != nil {
		log.ZError(ctx, "发送过期交易通知失败", err, "transaction_id", transaction.TransactionID, "sender_id", transaction.SenderID)
	}
}

// getTransactionTypeText 获取交易类型文本描述
func getTransactionTypeText(transactionType int) string {
	switch transactionType {
	case model.TransactionTypeTransfer:
		return "转账"
	case model.TransactionTypeP2PRedPacket:
		return "红包"
	case model.TransactionTypeNormalPacket:
		return "红包"
	case model.TransactionTypeLuckyPacket:
		return "红包"
	case model.TransactionTypeGroupExclusive:
		return "红包"
	case model.TransactionTypePasswordPacket:
		return "红包"

	default:
		return "未知类型"
	}
}

// ProcessExpiredTransactions 处理需要处理的过期交易
func (j *TransactionCronJob) ProcessExpiredTransactions(ctx context.Context) error {
	// 查找所有需要处理的过期交易
	transactions, err := j.transactionService.FindExpiredTransactions(ctx)
	if err != nil {
		log.ZError(ctx, "查询过期交易失败", err)
		return err
	}

	// 只有在找到交易时才记录日志
	if len(transactions) > 0 {
		log.ZInfo(ctx, "找到过期交易待处理", "count", len(transactions))
	} else {
		//log.ZInfo(ctx, "找到过期交易待处理", "count", len(transactions))
		return nil
	}

	// 限制并发数量，根据系统规模选择合适的并发数
	// 小型系统（单机）：5-10
	// 中型系统（集群）：10-20
	// 大型系统（高并发）：20-50
	const maxConcurrency = 15
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for _, transaction := range transactions {
		wg.Add(1)
		go func(t *model.Transaction) {
			defer wg.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }() // 释放信号量

			j.handleExpiredTransaction(ctx, t)
		}(transaction)
	}

	// 等待所有处理完成
	wg.Wait()

	// 只有在处理了交易时才记录完成日志
	if len(transactions) > 0 {
		log.ZInfo(ctx, "过期交易处理完成", "count", len(transactions))
	}
	return nil
}
