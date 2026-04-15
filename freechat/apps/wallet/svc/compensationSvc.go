package svc

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/apps/wallet/model"
	walletTsModel "github.com/openimsdk/chat/freechat/apps/walletTransactionRecord/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type CompensationSvc struct{}

func NewCompensationService() *CompensationSvc {
	return &CompensationSvc{}
}

// InitializeCompensationBalance 初始化用户的补偿金余额
// 同步执行，完成后才会返回
// 注意: 已从币种级别移到钱包级别，移除了currencyId参数
func (c *CompensationSvc) InitializeCompensationBalance(ctx context.Context, walletId primitive.ObjectID) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	settingsDao := model.NewWalletSettingsDao(db)
	walletInfoDao := model.NewWalletInfoDao(db)

	log.ZInfo(ctx, "Starting compensation balance initialization at wallet level",
		"walletId", walletId)

	// 获取钱包设置
	settings, err := settingsDao.GetDefaultSettings(ctx)
	if err != nil {
		log.ZError(ctx, "Failed to get wallet settings", err,
			"walletId", walletId)
		return err
	}

	// 检查补偿金功能是否启用
	if !settings.CompensationEnabled {
		log.ZInfo(ctx, "Compensation system disabled, skipping initialization",
			"walletId", walletId,
			"enabled", settings.CompensationEnabled)
		return nil
	}

	// 获取初始补偿金金额
	initialAmount, err := decimal.NewFromString(settings.InitialCompensation.String())
	if err != nil {
		log.ZError(ctx, "Failed to parse initial compensation amount", err,
			"walletId", walletId,
			"raw_amount", settings.InitialCompensation.String())
		return err
	}

	// 如果金额为0或负数，则跳过
	if initialAmount.Cmp(decimal.NewFromInt(0)) <= 0 {
		log.ZInfo(ctx, "Initial compensation amount is zero or negative, skipping initialization",
			"walletId", walletId,
			"amount", initialAmount.String())
		return nil
	}

	// 添加补偿金
	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		// 获取钱包信息
		walletInfo, err := walletInfoDao.GetById(sessionCtx, walletId)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.WalletNotOpenErr
			}
			return err
		}

		// 检查是否已有补偿金余额
		existingCompBalance, err := decimal.NewFromString(walletInfo.CompensationBalance.String())
		if err == nil && existingCompBalance.Cmp(decimal.NewFromInt(0)) > 0 {
			log.ZInfo(ctx, "Wallet already has compensation balance, skipping initialization",
				"walletId", walletId,
				"existingBalance", existingCompBalance.String())
			return nil
		}

		// 转换初始补偿金为Decimal128
		initialCompDecimal128, err := primitive.ParseDecimal128(initialAmount.String())
		if err != nil {
			log.ZError(ctx, "Failed to convert initial compensation to Decimal128", err,
				"walletId", walletId,
				"amount", initialAmount.String())
			return err
		}

		// 更新钱包的补偿金余额
		data := map[string]any{
			"compensation_balance": initialCompDecimal128,
			"updated_at":           walletInfo.UpdatedAt,
		}
		err = mongoutil.UpdateOne(sessionCtx, walletInfoDao.Collection,
			bson.M{"_id": walletId}, bson.M{"$set": data}, false)
		if err != nil {
			log.ZError(ctx, "Failed to update wallet compensation balance", err,
				"walletId", walletId,
				"amount", initialAmount.String())
			return err
		}

		// 添加交易记录
		// 补偿金与币种完全无关，不再使用USD币种

		amount128, err := primitive.ParseDecimal128(initialAmount.String())
		if err != nil {
			log.ZError(ctx, "Failed to parse amount for transaction record", err,
				"walletId", walletId,
				"amount", initialAmount.String())
			return err
		}

		// 创建交易记录
		err = walletTsModel.NewWalletTsRecordDao(db).Create(sessionCtx, &walletTsModel.WalletTransactionRecord{
			WalletId:        walletId,
			CurrencyId:      primitive.NilObjectID, // 补偿金与币种无关，使用空ID
			TransactionTime: walletInfo.UpdatedAt,
			Type:            walletTsModel.TsRecordTypeCompensationInitial,
			Amount:          amount128,
			Remark:          "初始补偿金",
			Source:          "system",
		})
		if err != nil {
			log.ZError(ctx, "Failed to create transaction record", err,
				"walletId", walletId,
				"amount", initialAmount.String())
			return err
		}

		log.ZInfo(ctx, "Compensation balance initialized successfully at wallet level",
			"walletId", walletId,
			"amount", initialAmount.String())

		return nil
	})

	// 返回最终结果
	if err != nil {
		log.ZError(ctx, "Compensation balance initialization failed", errs.Unwrap(err),
			"walletId", walletId)
		return errs.Unwrap(err)
	}

	log.ZInfo(ctx, "Compensation balance initialization completed successfully",
		"walletId", walletId)

	return nil
}

// DeductCompensationForCheckin 从补偿金余额扣除签到奖励金额
// 注意: 补偿金与币种完全无关
func (c *CompensationSvc) DeductCompensationForCheckin(ctx context.Context, walletId primitive.ObjectID, amount decimal.Decimal) (bool, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	settingsDao := model.NewWalletSettingsDao(db)
	walletInfoDao := model.NewWalletInfoDao(db)

	settings, err := settingsDao.GetDefaultSettings(ctx)
	if err != nil {
		log.ZError(ctx, "Get wallet settings error", err)
		return false, err
	}

	// 检查补偿金功能是否启用
	if !settings.CompensationEnabled {
		log.ZInfo(ctx, "Compensation system disabled, skipping deduction")
		return false, nil
	}

	deducted := false
	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		// 获取钱包信息
		walletInfo, err := walletInfoDao.GetById(sessionCtx, walletId)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.WalletNotOpenErr
			}
			return err
		}

		// 获取当前补偿金余额
		compensationBalance, err := decimal.NewFromString(walletInfo.CompensationBalance.String())
		if err != nil {
			compensationBalance = decimal.Zero
		}

		// 如果补偿金余额大于等于签到奖励，从补偿金扣除
		if compensationBalance.Cmp(amount) >= 0 {
			// 计算扣减后的余额
			newBalance := compensationBalance.Sub(amount)
			newBalanceDecimal128, err := primitive.ParseDecimal128(newBalance.String())
			if err != nil {
				return err
			}

			// 更新钱包的补偿金余额
			data := map[string]any{
				"compensation_balance": newBalanceDecimal128,
				"updated_at":           time.Now().UTC(),
			}
			err = mongoutil.UpdateOne(sessionCtx, walletInfoDao.Collection,
				bson.M{"_id": walletId}, bson.M{"$set": data}, false)
			if err != nil {
				return err
			}

			// 创建交易记录（仍然使用币种ID以保持与现有系统的兼容性）
			negativeAmount := amount.Neg() // 负数表示扣减
			amount128, err := primitive.ParseDecimal128(negativeAmount.String())
			if err != nil {
				return err
			}

			err = walletTsModel.NewWalletTsRecordDao(db).Create(sessionCtx, &walletTsModel.WalletTransactionRecord{
				WalletId:        walletId,
				CurrencyId:      primitive.NilObjectID, // 补偿金与币种无关，使用空ID
				TransactionTime: time.Now().UTC(),
				Type:            walletTsModel.TsRecordTypeCompensationDeduction,
				Amount:          amount128,
				Remark:          "签到奖励补偿金扣减",
				Source:          "checkin",
			})
			if err != nil {
				return err
			}

			deducted = true
			return nil
		}

		// 补偿金余额不足，无法扣除，返回false表示需要从系统扣除
		return nil
	})

	return deducted, errs.Unwrap(err)
}

// AdjustCompensationBalance 管理员调整补偿金余额
// 注意: 补偿金与币种完全无关
func (c *CompensationSvc) AdjustCompensationBalance(ctx context.Context, walletId primitive.ObjectID, amount decimal.Decimal) error {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	settingsDao := model.NewWalletSettingsDao(db)
	walletInfoDao := model.NewWalletInfoDao(db)

	settings, err := settingsDao.GetDefaultSettings(ctx)
	if err != nil {
		log.ZError(ctx, "Get wallet settings error", err)
		return err
	}

	// 检查补偿金功能是否启用
	if !settings.CompensationEnabled {
		return errs.New("Compensation system is disabled")
	}

	err = mongoCli.GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		// 获取钱包信息
		walletInfo, err := walletInfoDao.GetById(sessionCtx, walletId)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.WalletNotOpenErr
			}
			return err
		}

		// 获取当前补偿金余额
		currentBalance, err := decimal.NewFromString(walletInfo.CompensationBalance.String())
		if err != nil {
			currentBalance = decimal.Zero
		}

		// 检查调整后的余额是否小于0
		if currentBalance.Add(amount).Cmp(decimal.Zero) < 0 {
			return errs.New("Adjusted compensation balance cannot be negative")
		}

		// 计算新余额
		newBalance := currentBalance.Add(amount)
		newBalanceDecimal128, err := primitive.ParseDecimal128(newBalance.String())
		if err != nil {
			return err
		}

		// 更新钱包的补偿金余额
		data := map[string]any{
			"compensation_balance": newBalanceDecimal128,
			"updated_at":           time.Now().UTC(),
		}
		err = mongoutil.UpdateOne(sessionCtx, walletInfoDao.Collection,
			bson.M{"_id": walletId}, bson.M{"$set": data}, false)
		if err != nil {
			return err
		}

		// 创建交易记录（仍然使用币种ID以保持与现有系统的兼容性）
		amount128, err := primitive.ParseDecimal128(amount.String())
		if err != nil {
			return err
		}

		err = walletTsModel.NewWalletTsRecordDao(db).Create(sessionCtx, &walletTsModel.WalletTransactionRecord{
			WalletId:        walletId,
			CurrencyId:      primitive.NilObjectID, // 补偿金与币种无关，使用空ID
			TransactionTime: time.Now().UTC(),
			Type:            walletTsModel.TsRecordTypeCompensationAdjust,
			Amount:          amount128,
			Remark:          "管理员调整补偿金余额",
			Source:          "admin",
		})
		if err != nil {
			return err
		}

		return nil
	})

	return errs.Unwrap(err)
}

// GetCompensationBalance 获取用户的补偿金余额
// 注意: 补偿金与币种完全无关
func (c *CompensationSvc) GetCompensationBalance(ctx context.Context, walletId primitive.ObjectID) (decimal.Decimal, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	walletInfoDao := model.NewWalletInfoDao(db)

	// 获取钱包信息
	walletInfo, err := walletInfoDao.GetById(ctx, walletId)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return decimal.Zero, nil
		}
		return decimal.Zero, err
	}

	// 解析补偿金余额
	compensationBalance, err := decimal.NewFromString(walletInfo.CompensationBalance.String())
	if err != nil {
		return decimal.Zero, err
	}

	return compensationBalance, nil
}

// IsCompensationEnabled 检查补偿金功能是否启用
func (c *CompensationSvc) IsCompensationEnabled(ctx context.Context) (bool, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	settingsDao := model.NewWalletSettingsDao(db)

	settings, err := settingsDao.GetDefaultSettings(ctx)
	if err != nil {
		return false, err
	}

	return settings.CompensationEnabled, nil
}

// GetInitialCompensationAmount 获取初始补偿金金额
func (c *CompensationSvc) GetInitialCompensationAmount(ctx context.Context) (decimal.Decimal, error) {
	mongoCli := plugin.MongoCli()
	db := mongoCli.GetDB()
	settingsDao := model.NewWalletSettingsDao(db)

	settings, err := settingsDao.GetDefaultSettings(ctx)
	if err != nil {
		return decimal.Zero, err
	}

	initialAmount, err := decimal.NewFromString(settings.InitialCompensation.String())
	if err != nil {
		return decimal.Zero, err
	}

	return initialAmount, nil
}
