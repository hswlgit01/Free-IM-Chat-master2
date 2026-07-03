package model

import (
	"context"
	"sync"
	"time"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// UserLoginCity dawn 2026-07-03 异地登录限制：每个账号(im_server_user_id)绑定唯一登录城市。
// 普通会员登录时若本次 IP 城市与绑定城市不一致则拒绝；管理员在后台清除绑定后可重新绑定新城市。
type UserLoginCity struct {
	UserID  string    `bson:"user_id"` // im_server_user_id
	City    string    `bson:"city"`
	IP      string    `bson:"ip"`
	BoundAt time.Time `bson:"bound_at"`
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

// Upsert 绑定/更新账号的登录城市。
func (d *UserLoginCityDao) Upsert(ctx context.Context, userID, city, ip string) error {
	_, err := d.Collection.UpdateOne(ctx,
		bson.M{"user_id": userID},
		bson.M{"$set": bson.M{"city": city, "ip": ip, "bound_at": time.Now()}},
		options.Update().SetUpsert(true),
	)
	return err
}

// DeleteByUserID 后台清除账号登录城市绑定(清除后下次登录以新城市重新绑定)。
func (d *UserLoginCityDao) DeleteByUserID(ctx context.Context, userID string) error {
	_, err := d.Collection.DeleteOne(ctx, bson.M{"user_id": userID})
	return err
}
