package verifyCode

import (
	"context"
	"fmt"
	chatModel "github.com/openimsdk/chat/freechat/third/chat/model"
	"github.com/openimsdk/chat/pkg/common/config"
	"github.com/openimsdk/chat/pkg/common/constant"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	constantpb "github.com/openimsdk/chat/pkg/constant"
	"github.com/openimsdk/chat/pkg/eerrs"
	"github.com/openimsdk/chat/pkg/email"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"math/rand"
	"regexp"
	"time"
)

func genVerifyCode(codeLen int) string {
	data := make([]byte, codeLen)
	rand.Read(data)
	chars := []byte("0123456789")
	for i := 0; i < len(data); i++ {
		data[i] = chars[data[i]%10]
	}
	return string(data)
}

type SendVerifyCodeReqUsedFor int32

// verificationCode used for.
const (
	VerificationCodeForRegister      SendVerifyCodeReqUsedFor = 1 // Register
	VerificationCodeForResetPassword SendVerifyCodeReqUsedFor = 2 // Reset password
	VerificationCodeForLogin         SendVerifyCodeReqUsedFor = 3 // EmbedLogin
	VerificationCodeForPaymentPwd    SendVerifyCodeReqUsedFor = 4 // Payment password operations
	VerificationCodeForResetEmail    SendVerifyCodeReqUsedFor = 5 // 重置邮箱验证码
)

type SendVerifyCodeReq struct {
	VerifyCodeCfg config.VerifyCode

	UsedFor  SendVerifyCodeReqUsedFor
	Platform int32
	Email    string
}

func EmailCheck(email string) bool {
	pattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	compile := regexp.MustCompile(pattern)
	return compile.MatchString(email)
}

func SendEmailVerifyCode(ctx context.Context, db *mongo.Database, mail email.Mail, req *SendVerifyCodeReq) error {
	verifyCodeDao := chatModel.NewVerifyCodeDao(db)

	if req.VerifyCodeCfg.Mail.Use == constant.VerifySuperCode {
		return nil
	}

	if mail == nil {
		return errs.ErrArgs.WrapMsg("email interface is nil")
	}

	if !EmailCheck(req.Email) {
		return errs.ErrArgs.WrapMsg("email must be right")
	}

	if req.VerifyCodeCfg.Len <= 0 {
		return eerrs.ErrAccountNotFound.WrapMsg("email VerifyCodeLength not <= 0")
	}

	code := genVerifyCode(req.VerifyCodeCfg.Len)
	account := req.Email

	// 验证码入库
	now := time.Now()
	count, err := verifyCodeDao.RangeNum(ctx, account, now.Add(-86400), now)
	if err != nil {
		return err
	}
	if int(count) > req.VerifyCodeCfg.MaxCount {
		return eerrs.ErrVerifyCodeSendFrequently.Wrap()
	}
	platformName := constantpb.PlatformIDToName(int(req.Platform))
	if platformName == "" {
		platformName = fmt.Sprintf("platform:%d", req.Platform)
	}

	if err = mail.SendMail(ctx, req.Email, code); err != nil {
		return err
	}
	vc := &chatModel.VerifyCode{
		Account:    account,
		Code:       code,
		Platform:   platformName,
		Duration:   uint(req.VerifyCodeCfg.ValidTime),
		Count:      0,
		Used:       false,
		CreateTime: now,
		UsedFor:    int32(req.UsedFor),
	}
	err = verifyCodeDao.Add(ctx, []*chatModel.VerifyCode{vc})
	if err != nil {
		return err
	}

	log.ZDebug(ctx, "send code success", "account", account, "code", code, "platform", platformName)
	return nil
}

type VerifyEmailCodeReq struct {
	VerifyCodeCfg config.VerifyCode

	UsedFor           SendVerifyCodeReqUsedFor
	Email             string
	VerifyCode        string
	DeleteAfterVerify bool
}

func VerifyEmailCode(ctx context.Context, db *mongo.Database, req *VerifyEmailCodeReq) (primitive.ObjectID, error) {
	verifyCodeDao := chatModel.NewVerifyCodeDao(db)
	if req.VerifyCodeCfg.Mail.Use == constant.VerifySuperCode {
		if req.VerifyCode != req.VerifyCodeCfg.SuperCode {
			return primitive.NilObjectID, eerrs.ErrVerifyCodeNotMatch.Wrap()
		}
		return primitive.NilObjectID, nil
	}

	last, err := verifyCodeDao.TakeLast(ctx, req.Email, int32(req.UsedFor))
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return primitive.NilObjectID, eerrs.ErrVerifyCodeExpired.Wrap()
		}
		return primitive.NilObjectID, err
	}
	if last.CreateTime.Unix()+int64(last.Duration) < time.Now().Unix() {
		return last.ID, eerrs.ErrVerifyCodeExpired.Wrap()
	}
	if last.Used {
		return last.ID, eerrs.ErrVerifyCodeUsed.Wrap()
	}
	if n := req.VerifyCodeCfg.ValidCount; n > 0 {
		if last.Count >= n {
			return last.ID, eerrs.ErrVerifyCodeMaxCount.Wrap()
		}
		if last.Code != req.VerifyCode {
			if err := verifyCodeDao.Incr(ctx, last.ID); err != nil {
				return last.ID, err
			}
		}
	}
	if last.Code != req.VerifyCode {
		return last.ID, eerrs.ErrVerifyCodeNotMatch.Wrap()
	}

	if req.DeleteAfterVerify {
		if err = verifyCodeDao.Delete(ctx, last.ID); err != nil {
			log.ZWarn(ctx, "failed to delete verify code after verification", err, "codeID", last.ID)
		}
	}

	return last.ID, nil
}
