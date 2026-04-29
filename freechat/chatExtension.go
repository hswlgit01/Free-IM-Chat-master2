package freechat

import (
	"context"
	"fmt"

	"github.com/openimsdk/chat/freechat/apps/appLog"
	"github.com/openimsdk/chat/freechat/apps/article"
	// dawn 2026-04-27 引入临时撤回排查日志收集端点 /debug/log，用完整模块删除
	"github.com/openimsdk/chat/freechat/apps/debugLog"
	"github.com/openimsdk/chat/freechat/apps/defaultFriend"
	"github.com/openimsdk/chat/freechat/apps/defaultGroup"

	"github.com/openimsdk/chat/freechat/apps/platformConfig"

	"github.com/openimsdk/chat/freechat/apps/checkin"
	"github.com/openimsdk/chat/freechat/apps/lottery"
	"github.com/openimsdk/chat/freechat/apps/operationLog"
	"github.com/openimsdk/chat/freechat/apps/points"
	"github.com/openimsdk/chat/freechat/apps/systemStatistics"
	"github.com/openimsdk/chat/freechat/apps/user"

	constantpb "github.com/openimsdk/protocol/constant"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/apps/account"
	adminApp "github.com/openimsdk/chat/freechat/apps/admin"
	"github.com/openimsdk/chat/freechat/apps/exchangeRate"
	"github.com/openimsdk/chat/freechat/apps/group"
	"github.com/openimsdk/chat/freechat/apps/identity"
	"github.com/openimsdk/chat/freechat/apps/livestream"
	"github.com/openimsdk/chat/freechat/apps/networkRoute"
	"github.com/openimsdk/chat/freechat/apps/organization"
	organizationModel "github.com/openimsdk/chat/freechat/apps/organization/model"
	"github.com/openimsdk/chat/freechat/apps/paymentMethod"
	paymentMethodModel "github.com/openimsdk/chat/freechat/apps/paymentMethod/model"
	"github.com/openimsdk/chat/freechat/apps/rtc"
	"github.com/openimsdk/chat/freechat/apps/transaction"
	"github.com/openimsdk/chat/freechat/apps/userKeys"
	"github.com/openimsdk/chat/freechat/apps/wallet"
	walletSvc "github.com/openimsdk/chat/freechat/apps/wallet/svc"
	"github.com/openimsdk/chat/freechat/apps/walletTransactionRecord"
	"github.com/openimsdk/chat/freechat/apps/webhook"
	"github.com/openimsdk/chat/freechat/apps/withdrawal"
	withdrawalModel "github.com/openimsdk/chat/freechat/apps/withdrawal/model"
	depconstant "github.com/openimsdk/chat/freechat/constant"

	depmw "github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/ginUtils"
	chatmw "github.com/openimsdk/chat/internal/api/mw"
	"github.com/openimsdk/chat/pkg/common/imapi"
	"github.com/openimsdk/chat/pkg/common/mctx"
	"github.com/openimsdk/chat/pkg/protocol/admin"
	chatpb "github.com/openimsdk/chat/pkg/protocol/chat"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/chat/tools/db/redisutil"
	"github.com/openimsdk/tools/log"
	"github.com/shopspring/decimal"
)

// 全局变量保存定时任务实例，防止被垃圾回收
var transactionCronJob *transaction.TransactionCronJob
var exchangeRateCronJob *exchangeRate.ExchangeRateCronJob
var livestreamCronJob *livestream.LivestreamCronJob

// initPlugin 初始化插件
func initPlugin(cfg *plugin.ChatConfig, chatClient chatpb.ChatClient, adminClient admin.AdminClient, imApiCaller imapi.CallerInterface) error {
	// 初始化Redis客户端
	redisCli, err := redisutil.NewRedisClient(context.TODO(), cfg.RedisConfig.Build())
	if err != nil {
		return err
	}

	// 初始化MongoDB客户端
	mgocli, err := mongoutil.NewMongoDB(context.TODO(), cfg.MongodbConfig.Build())
	if err != nil {
		return err
	}

	// 初始化插件
	plugin.InitChatConfig(cfg)
	plugin.InitMongoCli(mgocli)
	plugin.InitChatClient(chatClient)
	plugin.InitAdminClient(adminClient)
	plugin.InitApiCaller(imApiCaller)
	plugin.InitRedisCli(redisCli)
	plugin.InitMail(cfg)
	return nil
}

// createDatabaseIndex 创建数据库索引（当前整段实现已注释，启动不再执行；需要时恢复下方块内代码并在 RegisterChatExtension 中取消对 createDatabaseIndex 的调用注释）
func createDatabaseIndex() error {
	return nil
	/*
		db := plugin.MongoCli().GetDB()
		if err := walletModel.CreateWalletBalanceIndex(db); err != nil {
			// 在部分开发/迁移环境中，可能存在历史重复数据导致钱包余额索引创建失败。
			// 为避免直接中断服务启动，这里仅记录告警日志并跳过该索引的强制创建。
			log.ZWarn(context.Background(), "failed to create wallet balance index, skip in dev env", err)
		}
		if err := transactionModel.CreateTransactionIndexes(db); err != nil {
			return fmt.Errorf("failed to create transaction index, %v", err)
		}
		if err := transactionModel.CreateRefundIndexes(db); err != nil {
			return fmt.Errorf("failed to create refund index, %v", err)
		}
		if err := walletTransactionRecordModel.CreateTransactionRecordIndex(db); err != nil {
			return fmt.Errorf("failed to create transaction record index, %v", err)
		}
		if err := userKeysModel.CreateUserKeysIndexes(db); err != nil {
			return fmt.Errorf("failed to create user keys index, %v", err)
		}
		if err := livestreamModel.CreateLivestreamStatisticsIndex(db); err != nil {
			return fmt.Errorf("failed to create livestream systemStatistics index, %v", err)
		}
		if err := walletModel.CreateWalletInfoIndex(db); err != nil {
			return fmt.Errorf("failed to create wallet info index, %v", err)
		}
		if err := walletModel.CreateWalletCurrencyIndex(db); err != nil {
			return fmt.Errorf("failed to create wallet currency index, %v", err)
		}
		if err := organizationModel.CreateOrganizationIndex(db); err != nil {
			return fmt.Errorf("failed to create organization index, %v", err)
		}
		if err := organizationModel.CreateOrganizationRolePermissionIndex(db); err != nil {
			return fmt.Errorf("failed to create organization config index, %v", err)
		}
		if err := webhookModel.CreateWebhookIndex(db); err != nil {
			return fmt.Errorf("failed to create webhook index, %v", err)
		}
		if err := webhookModel.CreateWebhookTriggerIndex(db); err != nil {
			return fmt.Errorf("failed to create webhook trigger index, %v", err)
		}
		if err := organizationModel.CreateOrganizationUserIndex(db); err != nil {
			return fmt.Errorf("failed to create organization user index, %v", err)
		}
		if err := organizationModel.CreateUserTagIndex(db); err != nil {
			return fmt.Errorf("failed to create user tag index, %v", err)
		}
		// 创建user表优化索引（跳过冲突的user_id索引）
		if err := openImUserModel.CreateUserIndexes(db); err != nil {
			return fmt.Errorf("failed to create user index, %v", err)
		}
		if err := lotteryModel.CreateLotteryUserRecordIndex(db); err != nil {
			return fmt.Errorf("failed to create lottery user record index, %v", err)
		}
		if err := operationLogModel.CreateOperationLogIndex(db); err != nil {
			return fmt.Errorf("failed to create operation log index, %v", err)
		}
		if err := checkinModel.CreateCheckinIndex(db); err != nil {
			return fmt.Errorf("failed to create checkin index, %v", err)
		}
		if err := checkinModel.CreateCheckinRewardIndex(db); err != nil {
			return fmt.Errorf("failed to create checkin reward index, %v", err)
		}
		if err := lotteryModel.CreateLotteryIndex(db); err != nil {
			return fmt.Errorf("failed to create lottery index, %v", err)
		}
		if err := lotteryModel.CreateLotteryConfigIndex(db); err != nil {
			return fmt.Errorf("failed to create lottery config index, %v", err)
		}
		if err := lotteryModel.CreateLotteryUserTicketIndex(db); err != nil {
			return fmt.Errorf("failed to create lottery user ticket index, %v", err)
		}
		if err := articleModel.CreateArticleIndex(db); err != nil {
			return fmt.Errorf("failed to create article index, %v", err)
		}

		return nil
	*/
}

// initCronJobs 初始化并启动所有定时任务
func initCronJobs() {
	// 初始化并启动交易过期处理定时任务
	transactionCronJob = transaction.NewTransactionCronJob()
	transactionCronJob.Start()
	log.ZInfo(context.Background(), "交易过期处理定时任务已成功启动")

	// 初始化并启动汇率获取定时任务  开发环境不需要更新频率太高
	exchangeRateCronJob = exchangeRate.NewExchangeRateCronJob()
	exchangeRateCronJob.Start()
	log.ZInfo(context.Background(), "汇率获取定时任务已成功启动")

	livestreamCronJob = livestream.NewLivestreamCronJob()
	livestreamCronJob.Start()
	log.ZInfo(context.Background(), "直播间定时任务已启动")
}

// registerDepRouter 注册dep服务路由
func registerDepRouter(router *gin.Engine) {
	// 初始化控制器
	depRouter := router.Group("/third")
	chatMiddleware := chatmw.New(plugin.AdminClient())

	// 注册钱包相关路由
	walletCtl := wallet.NewWalletCtl()
	walletCurrencyCtl := wallet.NewWalletCurrencyCtl()
	{
		walletApi := depRouter.Group("/wallet")
		walletApi.GET("/exist", chatMiddleware.CheckToken, walletCtl.GetWalletExist)                                                                   // 钱包是否开通
		walletApi.GET("/balance", chatMiddleware.CheckToken, walletCtl.GetWalletInfo)                                                                  // 获取当前钱包信息,包括部分余额
		walletApi.POST("/create", chatMiddleware.CheckToken, depmw.DecryptMiddleware(), walletCtl.PostWallet)                                          // 开通钱包
		walletApi.POST("/pay_pwd/update", chatMiddleware.CheckToken, depmw.DecryptMiddleware(), walletCtl.PostUpdatePayPwd)                            // 修改支付密码
		walletApi.POST("/pay_pwd/update_by_verify_code", chatMiddleware.CheckToken, depmw.DecryptMiddleware(), walletCtl.PostUpdatePayPwdByVerifyCode) // 修改支付密码
		//walletApi.POST("/balance/recharge/test", chatMiddleware.CheckToken, walletCtl.PostRechargeTest)                                                // 充值测试币
		walletApi.GET("/currencies", walletCurrencyCtl.GetWalletCurrencies)                                    // 查询所有代币信息
		walletApi.POST("/compensation/init", chatMiddleware.CheckToken, walletCtl.PostTriggerCompensationInit) // 触发补偿金初始化
	}

	// 注册钱包余额相关路由
	{
		walletBalanceCtl := wallet.NewWalletBalanceCtl()
		walletBalanceApi := depRouter.Group("/wallet_balance")
		walletBalanceApi.GET("/get_all_balance", chatMiddleware.CheckToken, walletBalanceCtl.GetAllBalance) // 展示用户所有组织钱包的余额信息(新版获取钱包余额接口)
		walletBalanceApi.GET("/get_balance", chatMiddleware.CheckToken, walletBalanceCtl.GetBalance)        // 展示用户某个组织钱包的余额信息(新版获取钱包余额接口)
	}

	// 钱包交易记录相关路由
	walletTsRecordCtl := walletTransactionRecord.NewWalletTsRecordCtl()
	{
		walletTsRecordApi := depRouter.Group("/walletTsRecord", chatMiddleware.CheckToken)
		walletTsRecordApi.GET("/ts/detail", walletTsRecordCtl.GetWalletTsRecord) // 获取单条交易记录详情
		walletTsRecordApi.GET("/ts", walletTsRecordCtl.ListWalletTsRecord)       // 批量获取交易记录详情
	}

	// 认证相关路由
	accountCtl := account.NewAccountCtl()
	{
		accountApi := depRouter.Group("/account")
		accountApi.POST("/compare", chatMiddleware.CheckToken, accountCtl.PostComparePwd) // 对比用户密码是否正确
		accountApi.POST("/embed/login", accountCtl.PostEmbedLogin)                        // 用户嵌入式登录
		accountApi.POST("/have_pwd", chatMiddleware.CheckToken, accountCtl.PostHavePwd)   // 用户是否拥有密码
	}

	// 验证码相关路由
	captchaCtl := account.NewCaptchaCtl()
	{
		captchaApi := depRouter.Group("/captcha")
		captchaApi.GET("/image", captchaCtl.GenCaptcha) // 获取数学算数图形验证码
	}

	// 用户相关路由
	userCtl := user.NewUserCtl()
	{
		accountApi := depRouter.Group("/user")
		accountApi.POST("/register", userCtl.WebPostRegisterUserByEmail)                                // 通过邮箱和群组邀请码注册用户
		accountApi.POST("/register_via_account", userCtl.WebPostRegisterUserByAccount)                  // 通过账号和群组邀请码注册用户
		accountApi.POST("/check_account_exists", userCtl.CheckAccountExists)                            // 检查账户是否已存在（不区分大小写）
		accountApi.POST("/update_info", chatMiddleware.CheckToken, userCtl.PostUpdateUserInfo)          // 修改用户信息
		accountApi.POST("/find/full", chatMiddleware.CheckToken, userCtl.FindUserFullInfo)              // 根据ImUserId查询用户完整信息
		accountApi.POST("/search/full", chatMiddleware.CheckToken, userCtl.SearchUserFullInfo)          // 搜索用户完整信息（头像昵称从IM表获取）
		accountApi.POST("/change_email", chatMiddleware.CheckToken, userCtl.ChangeEmail)                // 修改用户邮箱
		accountApi.POST("/login_record", chatMiddleware.CheckToken, userCtl.GetLoginRecordByImServerId) // 根据IMServerID查询用户登录记录
	}

	// 注册用户团队相关路由
	userTeamCtl := user.NewUserTeamCtl()
	user.RegisterUserTeamRoutes(depRouter, userTeamCtl)
	// 用户密钥相关路由
	userKeysCtl := userKeys.NewUserKeysCtl()
	{
		userKeysApi := depRouter.Group("/user_keys")
		userKeysApi.Use(chatMiddleware.CheckToken)
		userKeysApi.POST("/setup", userKeysCtl.SetupUserKeys) // 设置用户密钥
	}

	// App 客户端日志上传
	{
		appLogCtl := appLog.NewAppLogCtl()
		appLogApi := depRouter.Group("/app_log")
		appLogApi.POST("/upload", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), appLogCtl.Upload)
	}

	// 身份认证相关路由
	identityCtl := identity.NewIdentityCtl()
	{
		identityApi := depRouter.Group("/user/identity")
		identityApi.Use(chatMiddleware.CheckToken)
		identityApi.POST("/submit", identityCtl.SubmitIdentity) // 提交身份认证
		identityApi.GET("/info", identityCtl.GetIdentityInfo)   // 获取身份认证信息
	}

	// 收款方式相关路由
	paymentMethodCtl := paymentMethod.NewPaymentMethodCtl()
	{
		// 初始化PaymentMethod DAO
		if err := paymentMethodModel.InitPaymentMethodDao(); err != nil {
			log.ZError(context.Background(), "初始化PaymentMethod DAO失败", err)
		}

		paymentMethodApi := depRouter.Group("/user/payment-methods")
		paymentMethodApi.Use(chatMiddleware.CheckToken)
		paymentMethodApi.GET("", paymentMethodCtl.GetPaymentMethods)                    // 获取支付方式列表
		paymentMethodApi.POST("", paymentMethodCtl.CreatePaymentMethod)                 // 创建支付方式
		paymentMethodApi.POST("/:id/default", paymentMethodCtl.SetDefaultPaymentMethod) // 设置默认支付方式
		paymentMethodApi.POST("/:id/delete", paymentMethodCtl.DeletePaymentMethod)      // 删除支付方式
	}

	// 提现相关路由
	withdrawalCtl := withdrawal.NewWithdrawalCtl()
	{
		// 初始化Withdrawal DAO
		if err := withdrawalModel.InitWithdrawalDao(); err != nil {
			log.ZError(context.Background(), "初始化Withdrawal DAO失败", err)
		}

		withdrawalApi := depRouter.Group("/wallet/withdrawal")
		withdrawalApi.Use(chatMiddleware.CheckToken)
		withdrawalApi.GET("/rule", withdrawalCtl.GetWithdrawalRule)               // 获取提现规则
		withdrawalApi.POST("/submit", withdrawalCtl.SubmitWithdrawal)             // 提交提现申请
		withdrawalApi.GET("/records", withdrawalCtl.GetWithdrawalRecordList)      // 获取提现记录列表
		withdrawalApi.GET("/detail/:orderNo", withdrawalCtl.GetWithdrawalDetail)  // 获取提现详情
		withdrawalApi.POST("/cancel", withdrawalCtl.CancelWithdrawal)             // 取消提现
		withdrawalApi.GET("/check-pending", withdrawalCtl.CheckPendingWithdrawal) // 检查未处理的提现
	}

	// RTC 相关路由（音视频通话）
	rtcCtl := rtc.NewRtcCtl()
	{
		rtcApi := depRouter.Group("/rtc")
		rtcApi.Use(chatMiddleware.CheckToken)
		rtcApi.POST("/get_token", rtcCtl.PostGetTokenForVideoCall) // 获取一对一音视频通话的Token
	}

	transactionCtl := transaction.NewTransactionCtl()
	// 注册交易相关路由
	{
		transApi := depRouter.Group("/transaction")
		transApi.Use(chatMiddleware.CheckToken)

		transApi.POST("/create", depmw.DecryptMiddleware(), transactionCtl.CreateTransaction) // 创建交易
		transApi.POST("/receive", transactionCtl.ReceiveTransaction)                          // 领取交易
		transApi.POST("/check_received", transactionCtl.CheckUserReceived)                    // 查询用户是否接收了交易
		transApi.GET("/receive_details", transactionCtl.GetTransactionReceiveDetails)         // 查询交易接收详情
		transApi.GET("/receive_history", transactionCtl.GetUserReceiveHistory)                // 查询用户24小时内接收的交易记录
		transApi.POST("/check_completed", transactionCtl.CheckTransactionCompleted)           // 检查交易是否已领取完毕
		transApi.POST("/repair_consistency", transactionCtl.RepairTransactionConsistency)     // 修复交易数据一致性
		// 压测专用：始终注册路由，避免 404；未设置 STRESS_TEST_RECEIVE=1 时 handler 内返回 403
		depRouter.POST("/transaction/receive_stress", transactionCtl.ReceiveTransactionStress)
	}

	// 注册直播相关路由
	{
		livestreamCtl := livestream.NewLivestreamCtl()
		lsApi := depRouter.Group("/livestream")
		lsApi.POST("/create_stream", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostCreateStream) // 创建直播流（通过用户设备摄像头和麦克风）
		lsApi.POST("/join_stream", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostJoinStream) // 观众加入直播流
		lsApi.POST("/invite_to_stage", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostInviteToStage) // 邀请观众上台
		lsApi.POST("/remove_from_stage", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostRemoveFromStage) // 将参与者从舞台移除
		lsApi.POST("/block_viewer", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostBlockViewer) // 主播屏蔽观众
		lsApi.POST("/approve_hand_raise", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostApproveHandRaise) // 主播批准观众的举手请求
		lsApi.POST("/raise_hand", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostRaiseHandRaise) // 主播举手
		lsApi.POST("/stop_stream", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostStopStream) // 主播停止直播流
		lsApi.POST("/set_admin", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostSetAdminRole) // 主播设置房管
		lsApi.POST("/revoke_admin", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamCtl.WebPostRevokeAdminRole) // 取消管理员

		//lsApi.POST("/record/start", livestreamCtl.WebPostStartRecording) // 开始录屏
		//lsApi.POST("/record/stop", livestreamCtl.WebPostStopRecording)   // 停止录屏
		//lsApi.POST("/record/list", livestreamCtl.WebPostListRecording)   // 获取当前的录屏列表
	}

	// 注册直播统计相关路由
	{
		livestreamStatisticsCtl := livestream.NewLivestreamStatisticsCtl()
		lsApi := depRouter.Group("/livestream_statistics")
		lsApi.GET("/single", chatMiddleware.CheckToken, livestreamStatisticsCtl.WebGetLivestreamStatistics) // 获取单个房间统计记录
		lsApi.GET("/list", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), livestreamStatisticsCtl.WebGetListLivestreamStatistics) // 批量获取房间统计记录
	}

	// 添加汇率相关路由
	exchangeRateCtl := exchangeRate.NewExchangeRateCtl()
	{
		exchangeRateApi := depRouter.Group("/exchange_rate")
		exchangeRateApi.GET("/latest", exchangeRateCtl.GetLatestRates) // 获取最新汇率
	}

	orgCtl := organization.NewOrganizationCtl()
	{
		orgApi := depRouter.Group("/organization")
		orgApi.GET("/detail", chatMiddleware.CheckToken, orgCtl.GetOrganizationInfoById)                             // 根据组织id获取组织信息
		orgApi.POST("/join_using_invitation_code", chatMiddleware.CheckToken, orgCtl.PostJoinOrgUsingInvitationCode) // 通过邀请码加入组织

		// Register hierarchy routes
		hierarchyCtl := organization.NewHierarchyCtl()
		organization.RegisterHierarchyRoutes(orgApi, hierarchyCtl)
	}

	orgUserCtl := organization.NewOrganizationUserCtl()
	{
		orgUserApi := depRouter.Group("/organization_user")
		orgUserApi.GET("/get_self_all_org", chatMiddleware.CheckToken, orgUserCtl.GetSelfAllOrg)                      // 获取自身所有组织
		orgUserApi.POST("/change_org_user", chatMiddleware.CheckToken, orgUserCtl.PostChangeOrgUser)                  // 切换组织用户
		orgUserApi.GET("/get_org_by_im_server_user_id", chatMiddleware.CheckToken, orgUserCtl.GetOrgByImServerUserId) // 根据imServerUserId查询组织信息
	}

	orgRolePermissionCtl := organization.NewOrgRolePermissionCtl()
	{
		orgRolePermissionApi := depRouter.Group("/organization_role_permission")
		orgRolePermissionApi.GET("/get_self_org_role_permission", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), orgRolePermissionCtl.WebGetOrgRolePermission) // 获取当前用户组织角色权限

	}

	// 签到相关接口
	checkinRouter := depRouter.Group("/checkin")
	{
		checkinCtl := checkin.NewCheckinCtl()
		checkinRouter.POST("/create", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), checkinCtl.WebPostCreateCheckin) // 签到
		checkinRouter.GET("/detail", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), checkinCtl.WebGetDetailCheckin) // 获取用户签到情况
		checkinRouter.GET("/rule", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), checkinCtl.WebGetCheckinRule) // 获取签到规则
		checkinRouter.GET("/records-for-fix", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), checkinCtl.WebGetCheckinRecordsForFix) // 获取签到记录用于修复
		checkinRouter.POST("/fix", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), checkinCtl.WebPostFixCheckinRecords) // 修复签到记录
	}
	// 签到奖励相关接口
	checkinRewardRouter := depRouter.Group("/checkin_reward")
	{
		checkinRewardCtl := checkin.NewCheckinRewardCtl()
		checkinRewardRouter.GET("/list", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), checkinRewardCtl.WebGetListCheckinReward) // 查看当前组织签到奖励记录
	}

	// 抽奖相关接口
	lotteryRouter := depRouter.Group("/lottery")
	{
		lotteryCtl := lottery.NewLotteryCtl()
		lotteryRouter.GET("/detail", chatMiddleware.CheckToken, lotteryCtl.WebGetDetailLottery) // 查看抽奖活动详情
	}

	// 用户抽奖券相关接口
	lotteryUserTicketRouter := depRouter.Group("/lottery_user_ticket")
	{
		lotteryUserTicketCtl := lottery.NewLotteryUserTicketCtl()
		lotteryUserTicketRouter.GET("/detail", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), lotteryUserTicketCtl.WebGetListLotteryUserTicket) // 用户查看自己的抽奖券
		lotteryUserTicketRouter.POST("/use", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), lotteryUserTicketCtl.WebPostUseLotteryUserTicket) // 用户使用自己的抽奖券
	}

	// 用户抽奖记录相关接口
	lotteryUserRecordRouter := depRouter.Group("/lottery_user_record")
	{
		lotteryUserRecordCtl := lottery.NewLotteryUserRecordCtl()
		lotteryUserRecordRouter.POST("/list", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), lotteryUserRecordCtl.WebGetUserLotteryRecords) // 用户端查询抽奖记录
	}

	// 网络线路相关路由
	networkRouteCtl := networkRoute.NewNetworkRouteCtl()
	{
		networkApi := depRouter.Group("/network")
		networkApi.GET("/test/ping", networkRouteCtl.TestPing) // 测试ping端点
	}

	// 积分相关路由（用户端）
	pointsCtl := points.NewPointsCtl()
	{
		pointsApi := depRouter.Group("/points")
		pointsApi.POST("/list", chatMiddleware.CheckToken,
			depmw.CheckOrganization(organizationModel.AllOrganizationUserRole...), pointsCtl.QueryUserPoints) // 查询用户积分列表
	}

	// 文章公开访问接口（不需要权限校验）
	{
		articleCtl := article.NewArticleCtl()
		depRouter.GET("/article/:id", articleCtl.GetPublicArticleDetail) // 获取公开文章详情
	}

	// 压测接口 - 传递什么就返回什么，不需要权限校验
	depRouter.POST("/loadtest", func(c *gin.Context) {
		// 读取请求体
		body, err := c.GetRawData()
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to read request body"})
			return
		}

		// 设置响应头为原始Content-Type
		contentType := c.GetHeader("Content-Type")
		if contentType == "" {
			contentType = "application/json"
		}
		c.Header("Content-Type", contentType)

		// 直接返回原始数据，不包装
		c.Data(200, contentType, body)
	})
}

func registerDepAdminRouter(router *gin.Engine) {
	// 初始化控制器
	depAdminRouter := router.Group("/third_admin")
	chatMiddleware := chatmw.New(plugin.AdminClient())

	// 管理员认证相关路由
	{
		accountCtl := account.NewAccountCtl()
		depAdminRouter.POST("/login", accountCtl.PostOrgAdminLogin)                                             // 管理员登录
		depAdminRouter.POST("/two_factor_auth/send_verify_code", accountCtl.CmsPostTwoFactorAuthSendVerifyCode) // 管理员二次验证获取邮箱验证码
		depAdminRouter.POST("/two_factor_auth/login_via_email", accountCtl.CmsPostTwoFactorAuthLoginViaEmail)   // 管理员二次验证登录
	}
	// 群组管理相关接口
	groupRouter := depAdminRouter.Group("/group")
	{
		groupCtl := group.NewGroupCtl()
		groupRouter.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.CreateGroupWithOrg)                     // POST /dep_admin/group/create - 创建群组
		groupRouter.POST("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.GetGroupsByOrgID)                         // GET /dep_admin/group/list - 获取群组列表
		groupRouter.POST("/members", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.GetMembers)                            // 查看当前组织下某个群组所有成员
		groupRouter.POST("/members/add", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostInviteUserToGroup)             // 邀请组织成员加入群组
		groupRouter.POST("/dismiss", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostDismissGroup)                      // 解散当前组织下某个群组
		groupRouter.POST("/mute", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostMuteGroup)                            // 设置当前组织下某个群组全体禁言
		groupRouter.POST("/cancel_mute", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostCancelMuteGroup)               // 取消当前组织下某个群组全体禁言
		groupRouter.POST("/info/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostUpdateGroupInfo)               // 更新当前组织下某个群组信息
		groupRouter.POST("/members/info/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostUpdateGroupMemberInfo) // 更新当前组织下某个群组中某个成员的群组身份
		groupRouter.POST("/members/info/transfer", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostTransferGroup)       // 更新当前组织下某个群组的所有者
		groupRouter.POST("/members/mute", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostMuteGroupMember)              // 禁言当前组织下某个群组中某个成员
		groupRouter.POST("/members/cancel_mute", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostCancelMuteGroupMember) // 取消当前组织下某个群组中某个成员的禁言
		groupRouter.POST("/members/kick", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupCtl.PostKickGroupMember)              // 踢出当前组织下某个群组中某个成员
	}

	// 用户相关路由
	accountApi := depAdminRouter.Group("/user")
	{
		userCtl := user.NewUserCtl()
		accountApi.GET("/import_user_template_excel", userCtl.CmsGetImportUserExcelTemplateFile)                                           // 获取导入用户模板Excel文件
		accountApi.POST("/import_user_via_excel", chatMiddleware.CheckToken, depmw.CheckOrganization(), userCtl.CmsPostImportUserViaExcel) // 导入用户
	}

	blockRouter := depAdminRouter.Group("/block")
	{
		userCtl := user.NewUserCtl()
		blockRouter.POST("/add", chatMiddleware.CheckToken, depmw.CheckOrganization(), userCtl.BlackUser)          // Block user
		blockRouter.POST("/del", chatMiddleware.CheckToken, depmw.CheckOrganization(), userCtl.UnblockUser)        // Unblock user
		blockRouter.POST("/search", chatMiddleware.CheckToken, depmw.CheckOrganization(), userCtl.SearchBlockUser) // Search blocked users
	}

	ipBlockRouter := depAdminRouter.Group("/ip_block")
	{
		userCtl := user.NewUserCtl()
		ipBlockRouter.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), userCtl.CmsGetIPBlockList)       // Search IP blocks for registration/login
		ipBlockRouter.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), userCtl.CmsPostCreateIPBlock) // Add or update IP block
		ipBlockRouter.POST("/delete", chatMiddleware.CheckToken, depmw.CheckOrganization(), userCtl.CmsPostDeleteIPBlock) // Delete IP blocks
	}

	// 身份认证管理相关路由
	adminIdentityCtl := identity.NewAdminIdentityCtl()
	{
		identityRouter := depAdminRouter.Group("/identity")
		identityRouter.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminIdentityCtl.AdminGetIdentityList)     // 获取认证列表
		identityRouter.GET("/detail", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminIdentityCtl.AdminGetIdentityDetail) // 按 keyword 查单条实名详情（同 list 结构）

		identityRouter.POST("/approve", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminIdentityCtl.AdminApprove)            // 审核通过
		identityRouter.POST("/approve_batch", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminIdentityCtl.AdminApproveBatch) // 批量审核通过

		identityRouter.POST("/reject", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminIdentityCtl.AdminReject)             // 审核拒绝
		identityRouter.POST("/cancel", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminIdentityCtl.AdminCancelVerification) // 取消实名认证
	}

	// 提现管理相关路由
	adminWithdrawalCtl := withdrawal.NewAdminWithdrawalCtl()
	{
		withdrawalRouter := depAdminRouter.Group("/withdrawal")
		withdrawalRouter.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.GetWithdrawalList)         // 获取提现列表
		withdrawalRouter.GET("/detail/:id", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.GetWithdrawalDetail) // 获取提现详情
		withdrawalRouter.POST("/approve", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.ApproveWithdrawal)     // 审批通过
		withdrawalRouter.POST("/reject", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.RejectWithdrawal)       // 审批拒绝
		withdrawalRouter.POST("/transfer", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.TransferWithdrawal)   // 确认打款
		withdrawalRouter.POST("/complete", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.CompleteWithdrawal)   // 确认完成
		withdrawalRouter.POST("/batch-approve", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.BatchApprove)    // 批量审批
		withdrawalRouter.GET("/rule", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.GetWithdrawalRule)         // 获取提现规则
		withdrawalRouter.POST("/rule", chatMiddleware.CheckToken, depmw.CheckOrganization(), adminWithdrawalCtl.SaveWithdrawalRule)       // 保存提现规则
	}

	organizationCtl := organization.NewOrganizationCtl()
	{
		orgApi := depAdminRouter.Group("/organization")
		//orgApi.POST("/test/create", organizationCtl.PostTestCreate)                                                    // 创建组织-测试接口
		orgApi.POST("/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), organizationCtl.PostUpdate)       // 修改当前组织信息
		orgApi.GET("/info", chatMiddleware.CheckToken, depmw.CheckOrganization(), organizationCtl.GetOrganizationInfo) // 获取当前组织信息

		orgApi.POST("/wallet/exist", chatMiddleware.CheckToken, depmw.CheckOrganization(), organizationCtl.GetWalletExist)                           // 查询当前组织钱包是否已创建
		orgApi.POST("/wallet/currency/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), organizationCtl.PostCreateOrganizationCurrency) // 发布当前组织货币
		orgApi.POST("/wallet/currency/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), organizationCtl.PostUpdateOrganizationCurrency) // 编辑当前组织货币

		orgApi.POST("/update_checkin_rule", chatMiddleware.CheckToken, depmw.CheckOrganization(), organizationCtl.CmsPostUpdateCheckinRuleDescription) // 更新签到规则说明

		// 内部API：供Free-IM-Server调用（无需token验证）
		orgApi.GET("/internal/check_user_protection", organizationCtl.InternalCheckUserProtection) // 检查用户是否拥有官方账号保护权限
	}

	orgUserCtl := organization.NewOrganizationUserCtl()
	{
		orgUserApi := depAdminRouter.Group("/organization_user")
		orgUserApi.POST("/post_org_user", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.PostGetOrgUser)              // 查询组织用户（新版POST接口）
		orgUserApi.POST("/wallet_snapshot", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.PostOrgUserWalletSnapshot) // 批量钱包/补偿金（配合 omit_wallet 列表）

		orgUserApi.POST("/add_backend_admin", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.PostAddBackendAdmin)                 // 添加后台管理员
		orgUserApi.POST("/update_user_status", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.PostUpdateUserStatus)               // 修改用户状态
		orgUserApi.POST("/update_web_user_role", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.PostUpdateWebUserRole)            // 修改web用户角色,添加群组管理员
		orgUserApi.POST("/update_can_send_free_msg", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.PostUpdateUserCanSendFreeMsg) // 更新用户是否可自由发送消息
		orgUserApi.POST("/reset_password", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.PostResetOrgUserPassword)               // 组织后台重置组织用户密码
		orgUserApi.POST("/update_nickname", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.PostUpdateOrgUserNickname)             // 组织后台修改组织用户昵称
	}

	// 用户标签管理路由 - 集成到组织用户控制器中
	{
		userTagApi := depAdminRouter.Group("/user_tags")
		userTagApi.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.CreateUserTag)  // 创建标签
		userTagApi.POST("/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.UpdateUserTag)  // 更新标签
		userTagApi.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.GetUserTagList)    // 获取标签列表
		userTagApi.POST("/assign", chatMiddleware.CheckToken, depmw.CheckOrganization(), orgUserCtl.AssignUserTags) // 给用户打标签
	}

	orgRolePermissionCtl := organization.NewOrgRolePermissionCtl()
	{
		orgRolePermissionApi := depAdminRouter.Group("/organization_role_permission")
		orgRolePermissionApi.GET("/get_org_role_permission", chatMiddleware.CheckToken,
			depmw.CheckOrganization(), orgRolePermissionCtl.CmsGetOrgRolePermission) // 获取指定组织角色权限
		orgRolePermissionApi.POST("/update_org_role_permission", chatMiddleware.CheckToken,
			depmw.CheckOrganization(), orgRolePermissionCtl.CmsPostUpdateOrgRolePermission) // 修改指定用户组织角色权限

	}

	walletCtl := wallet.NewDepAdminWalletCtl()
	{
		orgWalletApi := depAdminRouter.Group("/organization/wallet")
		orgWalletApi.GET("/balance",
			chatMiddleware.CheckToken, depmw.CheckOrganization(), walletCtl.GetOrgBalance) // 查询当前组织钱包余额
		orgWalletApi.POST("/create",
			chatMiddleware.CheckToken, depmw.CheckOrganization(), depmw.DecryptMiddleware(), organizationCtl.PostCreateOrganizationWallet) // 创建当前组织钱包
		orgWalletApi.POST("/update",
			chatMiddleware.CheckToken, depmw.CheckOrganization(), depmw.DecryptMiddleware(), organizationCtl.PostUpdateOrganizationWallet) // 修改当前组织钱包密码

		// 补偿金系统设置
		compensationAdminSvc := walletSvc.NewCompensationAdminService()
		compensationGroup := orgWalletApi.Group("/compensation")
		compensationGroup.POST("/get_settings", chatMiddleware.CheckToken, depmw.CheckOrganization(), func(c *gin.Context) {
			// 从上下文中获取组织信息
			orgInfo, err := depmw.GetOrgInfoFromCtx(c)
			if err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 500, "errDlt": err.Error(), "data": nil})
				return
			}

			// 直接使用组织用户信息
			orgUser := orgInfo.OrgUser

			// 获取补偿金系统设置
			settings, err := compensationAdminSvc.GetCompensationSystemSettings(c, orgUser)
			if err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 500, "errDlt": err.Error(), "data": nil})
				return
			}

			// 返回响应
			resp := &admin.CompensationSettingsResp{
				Enabled:       settings.Enabled,
				InitialAmount: settings.InitialAmount.String(),
				NoticeText:    settings.NoticeText,
			}
			// 使用标准API响应格式，与前端期望的格式一致
			c.JSON(200, gin.H{
				"errCode": 0,
				"errDlt":  "",
				"data":    resp,
			})
		})
		compensationGroup.POST("/update_settings", chatMiddleware.CheckToken, depmw.CheckOrganization(), func(c *gin.Context) {
			// 解析请求
			var req admin.UpdateCompensationSettingsReq
			if err := c.ShouldBindJSON(&req); err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 400, "errDlt": "Invalid arguments", "data": nil})
				return
			}

			// 从上下文中获取组织信息
			orgInfo, err := depmw.GetOrgInfoFromCtx(c)
			if err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 500, "errDlt": err.Error(), "data": nil})
				return
			}

			// 直接使用组织用户信息
			orgUser := orgInfo.OrgUser

			// 解析初始补偿金金额
			initialAmount, err := decimal.NewFromString(req.InitialAmount)
			if err != nil {
				// 确保错误响应格式统一
				c.JSON(200, gin.H{"errCode": 400, "errDlt": "Invalid initial amount", "data": nil})
				return
			}

			// 更新补偿金系统设置
			err = compensationAdminSvc.UpdateCompensationSystemSettings(c, orgUser, &walletSvc.UpdateCompensationSystemSettingsReq{
				Enabled:       req.Enabled,
				InitialAmount: initialAmount,
				NoticeText:    req.NoticeText,
			})
			if err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 500, "errDlt": err.Error(), "data": nil})
				return
			}

			c.JSON(200, gin.H{"errCode": 0, "data": nil, "errDlt": ""})
		})
		compensationGroup.POST("/get_user_balance", chatMiddleware.CheckToken, depmw.CheckOrganization(), func(c *gin.Context) {
			// 解析请求
			var req admin.GetUserCompensationBalanceReq
			if err := c.ShouldBindJSON(&req); err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 400, "errDlt": "Invalid arguments", "data": nil})
				return
			}

			// 从上下文中获取组织信息
			orgInfo, err := depmw.GetOrgInfoFromCtx(c)
			if err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 500, "errDlt": err.Error(), "data": nil})
				return
			}

			// 直接使用组织用户信息
			orgUser := orgInfo.OrgUser

			// 获取用户补偿金余额
			userBalance, err := compensationAdminSvc.GetUserCompensationBalance(c, orgUser, req.UserID, req.CurrencyID)
			if err != nil {
				c.JSON(500, gin.H{"errCode": 500, "errDlt": err.Error()})
				return
			}

			// 返回响应
			resp := &admin.GetUserCompensationBalanceResp{
				UserID:              userBalance.UserID,
				Username:            userBalance.Username,
				WalletID:            userBalance.WalletID,
				CurrencyID:          userBalance.CurrencyID,
				CurrencyName:        userBalance.CurrencyName,
				CompensationBalance: userBalance.CompensationBalance.String(),
			}
			c.JSON(200, gin.H{"errCode": 0, "data": resp, "errDlt": ""})
		})

		compensationGroup.POST("/adjust_user_balance", chatMiddleware.CheckToken, depmw.CheckOrganization(), func(c *gin.Context) {
			// 解析请求
			var req admin.AdjustUserCompensationBalanceReq
			if err := c.ShouldBindJSON(&req); err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 400, "errDlt": "Invalid arguments", "data": nil})
				return
			}

			// 从上下文中获取组织信息
			orgInfo, err := depmw.GetOrgInfoFromCtx(c)
			if err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 500, "errDlt": err.Error(), "data": nil})
				return
			}

			// 直接使用组织用户信息
			orgUser := orgInfo.OrgUser

			// 解析调整金额
			amount, err := decimal.NewFromString(req.Amount)
			if err != nil {
				// 确保错误响应格式统一 - 使用HTTP 200状态码但包含错误信息
				c.JSON(200, gin.H{"errCode": 400, "errDlt": "Invalid amount", "data": nil})
				return
			}

			// 调整用户补偿金余额
			err = compensationAdminSvc.AdjustUserCompensationBalance(c, orgUser, &walletSvc.AdjustUserCompensationBalanceReq{
				UserID:     req.UserID,
				CurrencyID: req.CurrencyID,
				Amount:     amount,
				Reason:     req.Reason,
			})
			if err != nil {
				c.JSON(500, gin.H{"errCode": 500, "errDlt": err.Error()})
				return
			}

			c.JSON(200, gin.H{"errCode": 0, "data": nil, "errDlt": ""})
		})
	}
	// 钱包交易记录相关路由
	depAdminWalletTsRecordCtl := walletTransactionRecord.NewDepAdminWalletTsRecordCtl()
	{
		walletTsRecordApi := depAdminRouter.Group("/organization/wallet_ts_record", chatMiddleware.CheckToken)
		walletTsRecordApi.GET("/ts/detail", chatMiddleware.CheckToken, depmw.CheckOrganization(), depAdminWalletTsRecordCtl.GetOrgWalletTsRecord) // 获取单条交易记录详情
		walletTsRecordApi.GET("/ts", chatMiddleware.CheckToken, depmw.CheckOrganization(), depAdminWalletTsRecordCtl.ListOrgWalletTsRecord)       // 批量获取交易记录详情
	}

	webhookCtl := webhook.NewWebhookCtl()
	{
		webhookApi := depAdminRouter.Group("/webhook")
		webhookApi.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), webhookCtl.PostCreateWebhook) // 创建webhook
		webhookApi.POST("/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), webhookCtl.PostUpdateWebhook) // 编辑webhook
		webhookApi.POST("/delete", chatMiddleware.CheckToken, depmw.CheckOrganization(), webhookCtl.PostDeleteWebhook) // 删除webhook
		webhookApi.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), webhookCtl.GetWebhook)           // 批量获取webhook

	}

	webhookTriggerCtl := webhook.NewWebhookTriggerCtl()
	{
		webhookTriggerApi := depAdminRouter.Group("/webhook/trigger")
		webhookTriggerApi.GET("/list", chatMiddleware.CheckToken, webhookTriggerCtl.GetWebhookTriggerEvent) // 获取webhook所有事件
	}

	operationLogApi := depAdminRouter.Group("/operation_log")
	{
		groupOperationLogCtl := operationLog.NewGroupOperationLogCtl()
		operationLogApi.GET("/group/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), groupOperationLogCtl.CmsGetGroupOperationLog) // 获取群组操作日志

		operationLogCtl := operationLog.NewOperationLogCtl()
		operationLogApi.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), operationLogCtl.CmsGetListOperationLog) // 获取后台操作日志
	}

	appLogApi := depAdminRouter.Group("/app_log")
	{
		appLogCtl := appLog.NewAppLogCtl()
		appLogApi.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), appLogCtl.CmsList) // 获取 App 客户端日志
	}

	// 签到相关接口
	checkinRouter := depAdminRouter.Group("/checkin")
	{
		checkinCtl := checkin.NewCheckinCtl()
		checkinRouter.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinCtl.CmsGetListOrgUserCheckin)                       // 获取用户签到记录
		checkinRouter.POST("/supplement", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinCtl.CmsPostSupplementCheckin)                // 管理员补签（按时间段）
		checkinRouter.POST("/supplement_multiple", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinCtl.CmsPostSupplementMultipleDates) // 管理员多日期补签
		// 添加签到记录修复相关路由
		checkinRouter.GET("/records-for-fix", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinCtl.WebGetCheckinRecordsForFix) // 获取签到记录用于修复
		checkinRouter.POST("/fix", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinCtl.WebPostFixCheckinRecords)              // 修复指定用户最近一段连续签到记录
	}

	// 签到奖励相关接口
	checkinRewardRouter := depAdminRouter.Group("/checkin_reward")
	{
		checkinRewardCtl := checkin.NewCheckinRewardCtl()
		checkinRewardRouter.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinRewardCtl.CmsGetListCheckinReward)                         // 查看所有签到奖励审核
		checkinRewardRouter.POST("/update_status_apply", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinRewardCtl.CmsPostUpdateCheckinRewardApply) // 审批用户签到奖励
		checkinRewardRouter.POST("/fix_user_rewards", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinRewardCtl.CmsPostFixUserRewards)              // 修复指定用户的签到奖励数据（去重阶段奖励）
		checkinRewardRouter.POST("/fix_continuous_rewards", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinRewardCtl.CmsPostFixContinuousRewards)  // 修复当前组织下阶段性奖励去重（删除重复 15 元等）
	}

	// 连续签到奖励配置相关接口
	checkinRewardCfgRouter := depAdminRouter.Group("/checkin_reward_config")
	{
		checkinRewardCfgCtl := checkin.NewCheckinRewardCfgCtl()
		checkinRewardCfgRouter.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinRewardCfgCtl.CmsGetListCheckinRewardCfg)       // 展示所有连续签到奖励配置
		checkinRewardCfgRouter.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinRewardCfgCtl.CmsPostCreateCheckinRewardCfg) // 创建连续签到奖励配置
		checkinRewardCfgRouter.POST("/delete", chatMiddleware.CheckToken, depmw.CheckOrganization(), checkinRewardCfgCtl.CmsPostDeleteCheckinRewardCfg) // 删除连续签到奖励配置
	}

	// 日常签到奖励配置相关接口
	dailyCheckinRewardCfgRouter := depAdminRouter.Group("/daily_checkin_reward_config")
	{
		dailyCheckinRewardCfgCtl := checkin.NewDailyCheckinRewardCfgCtl()
		dailyCheckinRewardCfgRouter.GET("/detail", chatMiddleware.CheckToken, depmw.CheckOrganization(), dailyCheckinRewardCfgCtl.CmsGetDailyCheckinRewardCfg)                           // 获取日常签到奖励配置
		dailyCheckinRewardCfgRouter.POST("/create_or_update", chatMiddleware.CheckToken, depmw.CheckOrganization(), dailyCheckinRewardCfgCtl.CmsPostCreateOrUpdateDailyCheckinRewardCfg) // 创建或更新日常签到奖励配置
		dailyCheckinRewardCfgRouter.POST("/delete", chatMiddleware.CheckToken, depmw.CheckOrganization(), dailyCheckinRewardCfgCtl.CmsPostDeleteDailyCheckinRewardCfg)                   // 删除(禁用)日常签到奖励配置
	}

	// 抽奖相关接口
	lotteryRouter := depAdminRouter.Group("/lottery")
	{
		lotteryCtl := lottery.NewLotteryCtl()
		lotteryRouter.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryCtl.CmsGetListLottery)       // 查询抽奖活动
		lotteryRouter.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryCtl.CmsPostCreateLottery) // 创建抽奖活动
		lotteryRouter.POST("/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryCtl.CmsPostUpdateLottery) // 修改抽奖活动
		lotteryRouter.GET("/search", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryCtl.CmsGetSearchLottery)   // 查询搜索抽奖活动
	}

	lotteryRewardRouter := depAdminRouter.Group("/lottery_reward")
	{
		lotteryRewardCtl := lottery.NewLotteryRewardCtl()
		lotteryRewardRouter.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryRewardCtl.WebPostCreateLotteryReward)
		lotteryRewardRouter.POST("/modify", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryRewardCtl.WebPostUpdateLotteryReward)
		lotteryRewardRouter.POST("/rm/:id", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryRewardCtl.WebPostDeleteLotteryReward)
		lotteryRewardRouter.GET("/query", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryRewardCtl.WebGetSearchLotteryReward)
	}

	// 用户抽奖记录管理相关接口
	lotteryUserRecordAdminRouter := depAdminRouter.Group("/lottery_user_record")
	{
		lotteryUserRecordCtl := lottery.NewLotteryUserRecordCtl()
		lotteryUserRecordAdminRouter.POST("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryUserRecordCtl.CmsGetUserLotteryRecords)   // 管理端查询抽奖记录
		lotteryUserRecordAdminRouter.POST("/audit", chatMiddleware.CheckToken, depmw.CheckOrganization(), lotteryUserRecordCtl.CmsPostAuditLotteryRecord) // 管理员审核接口
	}

	//交易记录相关接口
	transApi := depAdminRouter.Group("/transaction")
	{
		transactionCtl := transaction.NewTransactionCtl()
		transApi.POST("/record", chatMiddleware.CheckToken, depmw.CheckOrganization(), transactionCtl.QueryTransactionRecords)     // 查询交易记录（重新实现）
		transApi.POST("/receive_record", chatMiddleware.CheckToken, depmw.CheckOrganization(), transactionCtl.QueryReceiveRecords) // 查询用户领取详情
	}

	// 积分管理相关接口
	pointsApi := depAdminRouter.Group("/points")
	{
		pointsCtl := points.NewPointsCtl()
		pointsApi.POST("/records", chatMiddleware.CheckToken, depmw.CheckOrganization(), pointsCtl.QueryPointsRecords) // 查询积分记录列表
	}

	statisticsApi := depAdminRouter.Group("/statistics")
	{
		statisticsCtl := systemStatistics.NewSystemStatisticsCtl()
		// 平台维度统计（平台累计数据）
		statisticsApi.GET("/system", chatMiddleware.CheckToken, depmw.CheckOrganization(), statisticsCtl.GetSystemStatistics)
		// 业务员每日新增统计（按邀请人维度）
		statisticsApi.GET("/sales_daily", chatMiddleware.CheckToken, depmw.CheckOrganization(), statisticsCtl.GetSalesDailyStatistics)
	}

	// 默认好友相关接口
	defaultFriendApi := depAdminRouter.Group("/default_friend")
	{
		defaultFriendCtl := defaultFriend.NewDefaultFriendCtl()
		defaultFriendApi.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), defaultFriendCtl.CmsGetListDefaultFriend)       // 获取默认好友列表
		defaultFriendApi.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), defaultFriendCtl.CmsPostCreateDefaultFriend) // 添加默认好友
		defaultFriendApi.POST("/delete", chatMiddleware.CheckToken, depmw.CheckOrganization(), defaultFriendCtl.CmsPostDeleteDefaultFriend) // 删除默认好友
		defaultFriendApi.GET("/search", chatMiddleware.CheckToken, depmw.CheckOrganization(), defaultFriendCtl.CmsGetSearchDefaultFriend)   // 搜索所有默认好友im id
	}

	// 默认群相关接口
	defaultGroupApi := depAdminRouter.Group("/default_group")
	{
		defaultGroupCtl := defaultGroup.NewDefaultGroupCtl()
		defaultGroupApi.GET("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), defaultGroupCtl.CmsGetListDefaultGroup)       // 获取默认群列表
		defaultGroupApi.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), defaultGroupCtl.CmsPostCreateDefaultGroup) // 添加默认群
		defaultGroupApi.POST("/delete", chatMiddleware.CheckToken, depmw.CheckOrganization(), defaultGroupCtl.CmsPostDeleteDefaultGroup) // 删除默认群
		defaultGroupApi.GET("/search", chatMiddleware.CheckToken, depmw.CheckOrganization(), defaultGroupCtl.CmsGetSearchDefaultGroup)   // 搜索所有默认群ID
	}

	// 通知账户相关接口
	notificationAccountCtl := organization.NewNotificationAccountCtl()
	{
		notificationAccountApi := depAdminRouter.Group("/notification_account")
		notificationAccountApi.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), notificationAccountCtl.CmsPostCreateNotificationAccount)  // 创建通知账户
		notificationAccountApi.POST("/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), notificationAccountCtl.CmsPostUpdateNotificationAccount)  // 更新通知账户
		notificationAccountApi.POST("/search", chatMiddleware.CheckToken, depmw.CheckOrganization(), notificationAccountCtl.CmsPostSearchNotificationAccount)  // 搜索通知账户
		notificationAccountApi.POST("/batch_send", chatMiddleware.CheckToken, depmw.CheckOrganization(), notificationAccountCtl.CmsPostSendBannerNotification) // 发送图文通知
	}

	// 文章管理相关接口
	articleCtl := article.NewArticleCtl()
	{
		articleApi := depAdminRouter.Group("/article")
		articleApi.POST("/create", chatMiddleware.CheckToken, depmw.CheckOrganization(), articleCtl.CmsPostCreateArticle)   // 创建文章
		articleApi.POST("/update", chatMiddleware.CheckToken, depmw.CheckOrganization(), articleCtl.CmsPostUpdateArticle)   // 更新文章
		articleApi.POST("/list", chatMiddleware.CheckToken, depmw.CheckOrganization(), articleCtl.CmsPostArticleList)       // 查询文章列表
		articleApi.GET("/detail/:id", chatMiddleware.CheckToken, depmw.CheckOrganization(), articleCtl.CmsGetArticleDetail) // 查询文章详情
	}

	// 层级管理相关接口
	{
		hierarchyCtl := organization.NewHierarchyCtl()
		organization.RegisterThirdAdminHierarchyRoutes(depAdminRouter, hierarchyCtl)
	}

	// 注册直播相关路由
	{
		//livestreamStatisticsCtl := livestream.NewLivestreamStatisticsCtl()
		//lsApi := depAdminRouter.Group("/livestream_statistics")
		//lsApi.GET("/list", livestreamStatisticsCtl.CmsGetListLivestreamStatistics)            // 批量获取房间统计记录
		//lsApi.POST("/record_file/detail", livestreamStatisticsCtl.CmsPostDetailRecordFileUrl) // 获取录播文件下载链接
	}

}

func registerSuperAdminRouter(router *gin.Engine) {
	superAdminRouter := router.Group("/super_admin")
	chatMiddleware := chatmw.New(plugin.AdminClient())

	// 签到奖励相关接口
	platformCfgRegisterSwitchRouter := superAdminRouter.Group("/platform_config/register_switch")
	{
		registerSwitchCtl := platformConfig.NewRegisterSwitchCtl()
		platformCfgRegisterSwitchRouter.GET("/", chatMiddleware.CheckAdmin, registerSwitchCtl.SuperCmsGetRegisterSwitch)
		platformCfgRegisterSwitchRouter.POST("/update", chatMiddleware.CheckAdmin, registerSwitchCtl.SuperCmsPostUpdateRegisterSwitch)
	}

	orgApi := superAdminRouter.Group("/organization")
	{
		organizationCtl := organization.NewOrganizationCtl()
		orgApi.GET("/list", chatMiddleware.CheckAdmin, organizationCtl.SuperCmsGetListOrg)       // 超管查询所有组织
		orgApi.POST("/create", chatMiddleware.CheckAdmin, organizationCtl.SuperCmsPostCreateOrg) // 超管创建组织
		orgApi.POST("/update", chatMiddleware.CheckAdmin, organizationCtl.SuperCmsPostUpdateOrg) // 超管编辑组织
	}

	// 用户管理相关接口
	userApi := superAdminRouter.Group("/user")
	{
		userCtl := user.NewUserCtl()
		userApi.POST("/list", chatMiddleware.CheckAdmin, userCtl.SuperAdminGetAllUsers)                 // 超管查询系统所有用户
		userApi.GET("/detail", chatMiddleware.CheckAdmin, userCtl.SuperAdminGetUserDetail)              // 超管查询用户详情
		userApi.POST("/reset_password", chatMiddleware.CheckAdmin, userCtl.SuperAdminResetUserPassword) // 超管重置用户密码
	}

	// 超管二次验证相关接口
	adminApi := superAdminRouter.Group("/two_factor_auth")
	{
		adminCtl := adminApp.NewAdminCtl()
		adminApi.POST("/send_verify_code", adminCtl.SuperCmsTwoFactorAuthSendVerifyCode) // 超管二次验证发送邮箱验证码
		adminApi.POST("/login_via_email", adminCtl.SuperCmsTwoFactorAuthLoginViaEmail)   // 超管二次验证邮箱登录
	}

	// 超管邮箱管理相关接口
	adminEmailApi := superAdminRouter.Group("/")
	{
		adminCtl := adminApp.NewAdminCtl()
		adminEmailApi.POST("/send_verify_code", chatMiddleware.CheckAdmin, adminCtl.SuperCmsPostSendEmailVerifyCode)     // 发送邮箱验证码
		adminEmailApi.POST("/set_email_with_verify", chatMiddleware.CheckAdmin, adminCtl.SuperCmsPostSetEmailWithVerify) // 通过验证码设置邮箱
		adminEmailApi.GET("/info", chatMiddleware.CheckAdmin, adminCtl.SuperCmsGetAdminInfo)                             // 获取超管信息
	}

	// 超管封禁用户相关接口
	forbiddenUserApi := superAdminRouter.Group("/forbidden_user")
	{
		adminCtl := adminApp.NewAdminCtl()
		forbiddenUserApi.POST("/forbid", chatMiddleware.CheckAdmin, adminCtl.SuperAdminForbidUser)           // 超管封禁用户
		forbiddenUserApi.POST("/unforbid", chatMiddleware.CheckAdmin, adminCtl.SuperAdminUnforbidUser)       // 超管解封用户
		forbiddenUserApi.POST("/search", chatMiddleware.CheckAdmin, adminCtl.SuperAdminSearchForbiddenUsers) // 超管搜索封禁用户
	}
}

// RegisterChatExtension 注册聊天扩展功能
func RegisterChatExtension(cfg *plugin.ChatConfig,
	router *gin.Engine,
	chatClient chatpb.ChatClient, adminClient admin.AdminClient, imApiCaller imapi.CallerInterface) {

	if err := initPlugin(cfg, chatClient, adminClient, imApiCaller); err != nil {
		panic(err)
	}

	// 已禁用：启动时创建 Mongo 索引（避免启动写库/耗时；需要时请恢复 createDatabaseIndex 实现及此处调用）
	// if err := createDatabaseIndex(); err != nil {
	// 	panic(err)
	// }

	// 初始化通知账户
	initNotificationAccount(imApiCaller)

	// 初始化并启动定时任务
	initCronJobs()

	registerDepRouter(router)

	registerDepAdminRouter(router)

	registerSuperAdminRouter(router)

	// dawn 2026-04-27 临时排查通道：客户端在撤回折叠失败时 POST /debug/log；
	// 我从外网带 ?key=im-revoke-debug-2026-04-27 拉 GET /debug/log。
	// 整批 bug 定位完成后整组路由 + apps/debugLog 一起删除。
	debugApi := router.Group("/debug")
	{
		debugApi.POST("/log", debugLog.PostHandler)
		debugApi.GET("/log", debugLog.GetHandler)
	}

	ginUtils.PrintRoutes(router, fmt.Sprintf("127.0.0.1:%d", cfg.ApiConfig.Api.Ports[0]))
}

// initNotificationAccount 初始化通知账户
func initNotificationAccount(imApiCaller imapi.CallerInterface) {
	ctx := context.Background()
	// 在服务内部获取管理员Token
	ctxWithOpID := context.WithValue(ctx, constantpb.OperationID, depconstant.NOTIFICATION_ADMIN_PAYMENT_SEND_ID)
	adminToken, err := imApiCaller.ImAdminTokenWithDefaultAdmin(ctxWithOpID)
	if err != nil {
		log.ZError(ctx, "获取IM管理员token失败", err)
	}
	// 构建通知账户信息
	imApiCaller.AddNotificationAccount(mctx.WithApiToken(ctxWithOpID, adminToken),
		depconstant.NOTIFICATION_ADMIN_PAYMENT_SEND_ID,
		depconstant.DefaultNotificationName,
		depconstant.DefaultNotificationFaceURL,
		"")
}
