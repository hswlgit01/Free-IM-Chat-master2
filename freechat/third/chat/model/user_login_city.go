package model

import (
	"context"
	"sync"
	"time"

	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/ip2regionUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// UserLoginCity dawn 2026-07-03 异地登录限制：每个账号(im_server_user_id)绑定唯一登录城市。
// 普通会员登录时若本次 IP 城市与绑定城市不一致则拒绝；管理员在后台清除绑定后可重新绑定新城市。
type UserLoginCity struct {
	UserID       string    `bson:"user_id"` // im_server_user_id
	City         string    `bson:"city"`
	IP           string    `bson:"ip"`
	BoundAt      time.Time `bson:"bound_at"`
	ResetPending bool      `bson:"reset_pending,omitempty"`
}

const (
	LoginCityReasonUnknownIP        = "unknown_ip"
	LoginCityReasonSameCity         = "same_city"
	LoginCityReasonBoundFromHistory = "bound_from_history"
	LoginCityReasonBoundFromCurrent = "bound_from_current"
	LoginCityReasonCityMismatch     = "city_mismatch"
	LoginCityReasonResetPending     = "reset_pending"
)

// LoginCityCheckResult describes the result of a city binding/check.  It is
// intentionally transport-agnostic so login flows and the internal WS check
// use exactly the same decision logic.
type LoginCityCheckResult struct {
	Allowed     bool   `json:"allowed"`
	Reason      string `json:"reason"`
	CurrentCity string `json:"current_city,omitempty"`
	BoundCity   string `json:"bound_city,omitempty"`
}

func (UserLoginCity) TableName() string {
	return "user_login_city"
}

type UserLoginCityDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

var userLoginCityIndexOnce sync.Once

func NewUserLoginCityDao(db *mongo.Database) *UserLoginCityDao {
	coll := db.Collection(UserLoginCity{}.TableName())
	// 索引自愈：进程内首次构造时补 user_id 唯一索引(非致命，失败忽略)。
	userLoginCityIndexOnce.Do(func() {
		_, _ = coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys:    bson.D{{Key: "user_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
	})
	return &UserLoginCityDao{DB: db, Collection: coll}
}

// GetByUserID 未绑定时返回 not found 错误(调用方用 dbutil.IsDBNotFound 判定)。
func (d *UserLoginCityDao) GetByUserID(ctx context.Context, userID string) (*UserLoginCity, error) {
	return mongoutil.FindOne[*UserLoginCity](ctx, d.Collection, bson.M{"user_id": userID})
}

// BindIfEmpty atomically establishes the first non-empty city and then reads
// the winning value back.  A plain upsert with $set is unsafe here: two first
// logins from different cities can both pass and the last writer silently
// changes the account's binding.
func (d *UserLoginCityDao) BindIfEmpty(ctx context.Context, userID, city, ip string) (*UserLoginCity, bool, error) {
	return d.bindIfEmpty(ctx, userID, city, ip, false)
}

// bindAfterCredentialReset consumes reset_pending only after the caller has
// authenticated with a password or verification code. Keeping this separate
// from BindIfEmpty closes the race where an old WS session observes a missing
// row just before an administrator writes the reset marker.
func (d *UserLoginCityDao) bindAfterCredentialReset(ctx context.Context, userID, city, ip string) (*UserLoginCity, bool, error) {
	return d.bindIfEmpty(ctx, userID, city, ip, true)
}

func (d *UserLoginCityDao) bindIfEmpty(ctx context.Context, userID, city, ip string, consumeReset bool) (*UserLoginCity, bool, error) {
	boundAt := time.Now()
	set := bson.M{"city": city, "ip": ip, "bound_at": boundAt, "reset_pending": false}

	// Support legacy rows whose city field exists but is empty.  Only one
	// concurrent caller can change the row from empty to non-empty.
	filter := bson.M{
		"user_id": userID,
		"$or": []bson.M{
			{"city": ""},
			{"city": bson.M{"$exists": false}},
		},
	}
	if !consumeReset {
		filter["reset_pending"] = bson.M{"$ne": true}
	}
	result, err := d.Collection.UpdateOne(ctx, filter, bson.M{"$set": set})
	if err != nil {
		return nil, false, err
	}
	won := result.ModifiedCount > 0

	if !won {
		// $setOnInsert makes an already-bound row immutable.  With the unique
		// user_id index, concurrent inserts converge on a single winner.
		result, err = d.Collection.UpdateOne(ctx,
			bson.M{"user_id": userID},
			bson.M{"$setOnInsert": bson.M{
				"user_id":       userID,
				"city":          city,
				"ip":            ip,
				"bound_at":      boundAt,
				"reset_pending": false,
			}},
			options.Update().SetUpsert(true),
		)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			return nil, false, err
		}
		if err == nil {
			won = result.UpsertedCount > 0
		}
	}

	binding, err := d.GetByUserID(ctx, userID)
	if err != nil {
		return nil, false, err
	}
	return binding, won, nil
}

// ResetByUserID records an explicit administrator reset. Keeping a marker is
// essential: a physical delete is indistinguishable from a pre-migration
// account and would cause the next credential login to restore the old city
// from user_login_record instead of binding the administrator-approved city.
func (d *UserLoginCityDao) ResetByUserID(ctx context.Context, userID string) error {
	_, err := d.Collection.UpdateOne(ctx,
		bson.M{"user_id": userID},
		bson.M{"$set": bson.M{
			"city":          "",
			"ip":            "",
			"bound_at":      time.Now(),
			"reset_pending": true,
		}},
		options.Update().SetUpsert(true),
	)
	return err
}

// DeleteByUserID 后台清除账号登录城市绑定(清除后下次登录以新城市重新绑定)。
func (d *UserLoginCityDao) DeleteByUserID(ctx context.Context, userID string) error {
	_, err := d.Collection.DeleteOne(ctx, bson.M{"user_id": userID})
	return err
}

// CheckAndBindLoginCity preserves the original API for callers that do not
// have the chat-side account ID. New login/session paths should use
// CheckAndBindLoginCityWithHistory so an old token cannot reverse-bind an
// empty table from a new location.
func CheckAndBindLoginCity(ctx context.Context, db *mongo.Database, imUserID, ip string) error {
	_, err := CheckAndBindLoginCityWithHistory(ctx, db, imUserID, "", ip)
	return err
}

// CheckExistingLoginCity performs the read-only phase used by multi-org
// login. bound=false means the caller may establish the binding only after all
// of the account's existing organization bindings have passed validation.
func CheckExistingLoginCity(ctx context.Context, db *mongo.Database, imUserID, ip string) (result *LoginCityCheckResult, bound bool, err error) {
	currentCity := ip2regionUtils.GetCityByIP(ip)
	if currentCity == "" {
		return &LoginCityCheckResult{Allowed: true, Reason: LoginCityReasonUnknownIP}, false, nil
	}
	binding, err := NewUserLoginCityDao(db).GetByUserID(ctx, imUserID)
	if err != nil {
		if dbutil.IsDBNotFound(err) {
			return &LoginCityCheckResult{
				Allowed: true, Reason: LoginCityReasonSameCity, CurrentCity: currentCity,
			}, false, nil
		}
		return nil, false, err
	}
	if binding.City == "" {
		return &LoginCityCheckResult{
			Allowed: true, Reason: LoginCityReasonSameCity, CurrentCity: currentCity,
		}, false, nil
	}
	result, err = compareLoginCities(currentCity, binding.City)
	return result, true, err
}

// CheckAndBindLoginCityWithHistory enforces the city restriction for one IM
// identity. accountUserID is the chat-side user_id used by user_login_record.
//
// If no city is bound, the latest historical login IP wins over the current
// connection IP. This matters for pre-existing tokens after rollout: their
// first WS reconnect from a new city must not establish that new city as the
// account's home. Unknown/private IPs never create or change a binding.
func CheckAndBindLoginCityWithHistory(
	ctx context.Context,
	db *mongo.Database,
	imUserID string,
	accountUserID string,
	ip string,
) (*LoginCityCheckResult, error) {
	return checkAndBindLoginCityWithHistory(ctx, db, imUserID, accountUserID, ip, true)
}

// CheckLoginCityForSession is the WS/old-token variant. It may restore a
// binding from authenticated login history, but it never lets an uncredentialed
// connection establish the current city when no history exists.
func CheckLoginCityForSession(
	ctx context.Context,
	db *mongo.Database,
	imUserID string,
	accountUserID string,
	ip string,
) (*LoginCityCheckResult, error) {
	return checkAndBindLoginCityWithHistory(ctx, db, imUserID, accountUserID, ip, false)
}

func checkAndBindLoginCityWithHistory(
	ctx context.Context,
	db *mongo.Database,
	imUserID string,
	accountUserID string,
	ip string,
	bindCurrentWithoutHistory bool,
) (*LoginCityCheckResult, error) {
	currentCity := ip2regionUtils.GetCityByIP(ip)

	cityDAO := NewUserLoginCityDao(db)
	binding, err := cityDAO.GetByUserID(ctx, imUserID)
	if err == nil && binding.ResetPending {
		if !bindCurrentWithoutHistory {
			return resetPendingResult(currentCity)
		}
		// Only a password/verification-code authenticated login consumes an
		// explicit reset, and it binds the current city rather than history.
		if currentCity == "" {
			return resetPendingResult(currentCity)
		}
		binding, won, bindErr := cityDAO.bindAfterCredentialReset(ctx, imUserID, currentCity, ip)
		if bindErr != nil {
			return nil, bindErr
		}
		if binding.ResetPending {
			return resetPendingResult(currentCity)
		}
		result, compareErr := compareLoginCities(currentCity, binding.City)
		if compareErr == nil && won {
			result.Reason = LoginCityReasonBoundFromCurrent
		}
		return result, compareErr
	}
	if currentCity == "" {
		return &LoginCityCheckResult{Allowed: true, Reason: LoginCityReasonUnknownIP}, nil
	}
	if err == nil && binding.City != "" {
		return compareLoginCities(currentCity, binding.City)
	}
	if err != nil && !dbutil.IsDBNotFound(err) {
		return nil, err
	}

	candidateCity, candidateIP, source, err := initialLoginCityCandidate(
		ctx, db, accountUserID, currentCity, ip, bindCurrentWithoutHistory,
	)
	if err != nil {
		return nil, err
	}
	// A historical record with an unresolvable IP is evidence that we do not
	// know the prior city. Do not reverse-bind it to the current WS location.
	if candidateCity == "" {
		return &LoginCityCheckResult{Allowed: true, Reason: LoginCityReasonUnknownIP, CurrentCity: currentCity}, nil
	}

	binding, won, err := cityDAO.BindIfEmpty(ctx, imUserID, candidateCity, candidateIP)
	if err != nil {
		return nil, err
	}
	if binding.ResetPending {
		return resetPendingResult(currentCity)
	}
	result, err := compareLoginCities(currentCity, binding.City)
	if err == nil && won {
		result.Reason = source
	}
	return result, err
}

func resetPendingResult(currentCity string) (*LoginCityCheckResult, error) {
	result := &LoginCityCheckResult{
		Allowed:     false,
		Reason:      LoginCityReasonResetPending,
		CurrentCity: currentCity,
	}
	return result, freeErrors.RemoteLoginCityErr("登录城市已由管理员重置，请使用账号密码或验证码重新登录。")
}

// initialLoginCityCandidate is separated from Mongo mutation so the fallback
// precedence can be covered with deterministic table tests.
func initialLoginCityCandidate(
	ctx context.Context,
	db *mongo.Database,
	accountUserID string,
	currentCity string,
	currentIP string,
	bindCurrentWithoutHistory bool,
) (city, ip, source string, err error) {
	if accountUserID == "" {
		if !bindCurrentWithoutHistory {
			return "", "", LoginCityReasonUnknownIP, nil
		}
		return currentCity, currentIP, LoginCityReasonBoundFromCurrent, nil
	}
	records, err := NewUserLoginRecordDao(db).FindRecentByUserID(ctx, accountUserID, 20)
	if err != nil {
		return "", "", "", err
	}
	if len(records) == 0 {
		if !bindCurrentWithoutHistory {
			return "", "", LoginCityReasonUnknownIP, nil
		}
		return currentCity, currentIP, LoginCityReasonBoundFromCurrent, nil
	}
	historyCity, historyIP := firstKnownLoginCity(records, ip2regionUtils.GetCityByIP)
	if historyCity == "" {
		return "", "", LoginCityReasonUnknownIP, nil
	}
	return historyCity, historyIP, LoginCityReasonBoundFromHistory, nil
}

func firstKnownLoginCity(records []*UserLoginRecord, resolve func(string) string) (city, ip string) {
	for _, record := range records {
		if record == nil {
			continue
		}
		if city := resolve(record.IP); city != "" {
			return city, record.IP
		}
	}
	return "", ""
}

func compareLoginCities(currentCity, boundCity string) (*LoginCityCheckResult, error) {
	result := &LoginCityCheckResult{
		Allowed:     currentCity == boundCity,
		Reason:      LoginCityReasonSameCity,
		CurrentCity: currentCity,
		BoundCity:   boundCity,
	}
	if result.Allowed {
		return result, nil
	}
	result.Reason = LoginCityReasonCityMismatch
	return result, freeErrors.RemoteLoginCityErr("异地登录已被限制：当前登录城市(" + currentCity +
		")与账号绑定城市(" + boundCity + ")不一致，如需异地登录请联系管理员在后台清除登录IP。")
}
