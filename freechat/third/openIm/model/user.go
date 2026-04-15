package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type User struct {
	ID               primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	UserID           string             `bson:"user_id"`
	Nickname         string             `bson:"nickname"`
	FaceURL          string             `bson:"face_url"`
	Ex               string             `bson:"ex"`
	AppMangerLevel   int32              `bson:"app_manger_level"`
	CanSendFreeMsg   int32              `bson:"can_send_free_msg"` // 新增：0=普通用户需好友验证，1=可跳过消息验证
	GlobalRecvMsgOpt int32              `bson:"global_recv_msg_opt"`
	CreateTime       time.Time          `bson:"create_time"`
	OrgId            string             `bson:"org_id"`   //组织ID
	OrgRole          string             `bson:"org_role"` //四种枚举 - "SuperAdmin", "BackendAdmin", "GroupManager", "Normal"
}

func (u User) TableName() string {
	return constant.CollectionUser
}

// CreateUserIndexes 为user表创建查询优化索引（跳过user_id，避免与其他服务冲突）
func CreateUserIndexes(db *mongo.Database) error {
	userModel := &User{}
	coll := db.Collection(userModel.TableName())

	// 只创建我们需要的优化索引，跳过user_id（其他服务已创建）
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		// 组织查询索引
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
			},
		},
		// 昵称搜索索引 - 支持模糊查询
		{
			Keys: bson.D{
				{Key: "nickname", Value: 1},
			},
		},
		// 组织内用户查询优化索引
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "user_id", Value: 1},
			},
		},
		// 消息权限查询索引
		{
			Keys: bson.D{
				{Key: "can_send_free_msg", Value: 1},
			},
		},
		// 组织角色查询索引
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "org_role", Value: 1},
			},
		},
		// 通知账户查询索引
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
				{Key: "app_manger_level", Value: 1},
				{Key: "create_time", Value: -1},
			},
		},
	})

	return err
}

type UserDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewUserDao(db *mongo.Database) *UserDao {
	userModel := User{}
	return &UserDao{
		DB:         db,
		Collection: db.Collection(userModel.TableName()),
	}
}

// Take 根据用户ID获取用户信息
func (u *UserDao) Take(ctx context.Context, imServerUserId string) (*User, error) {
	return mongoutil.FindOne[*User](ctx, u.Collection, bson.M{"user_id": imServerUserId})
}

// FindByUserIDs 批量根据用户ID获取用户信息
func (u *UserDao) FindByUserIDs(ctx context.Context, userIDs []string) ([]*User, error) {
	if len(userIDs) == 0 {
		return []*User{}, nil
	}

	filter := bson.M{
		"user_id": bson.M{"$in": userIDs},
	}

	users, err := mongoutil.Find[*User](ctx, u.Collection, filter)
	if err != nil {
		return nil, err
	}

	return users, nil
}

func (u *UserDao) FindFilter(ctx context.Context, filter bson.M, opts ...*options.FindOptions) ([]*User, error) {
	users, err := mongoutil.Find[*User](ctx, u.Collection, filter)
	if err != nil {
		return nil, err
	}

	return users, nil
}

func (u *UserDao) Select(ctx context.Context, userIds []string, page *paginationUtils.DepPagination) (int64, []*User, error) {
	filter := bson.M{}
	if len(userIds) > 0 {
		filter["user_id"] = bson.M{"$in": userIds}
	}

	opts := make([]*options.FindOptions, 0)
	// 默认用order排序
	opts = append(opts, options.Find().SetSort(bson.M{"create_time": 1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*User](ctx, u.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, u.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

// SearchNotificationAccounts 搜索组织的通知账户
func (u *UserDao) SearchNotificationAccounts(ctx context.Context, orgId string, keyword string, page *paginationUtils.DepPagination) (int64, []*User, error) {
	// 构建查询条件
	filter := bson.M{
		"org_id":           orgId,
		"app_manger_level": 3, // 通知账户标识
	}

	// 如果有关键字，添加昵称和userID的模糊搜索
	if keyword != "" {
		filter["$or"] = []bson.M{
			{"nickname": bson.M{"$regex": keyword, "$options": "i"}},
			{"user_id": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	// 执行查询
	opts := make([]*options.FindOptions, 0)
	opts = append(opts, options.Find().SetSort(bson.M{"create_time": -1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	users, err := mongoutil.Find[*User](ctx, u.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	// 获取总数
	total, err := mongoutil.Count(ctx, u.Collection, filter)
	if err != nil {
		return 0, nil, err
	}

	return total, users, nil
}

// GetOrgUserIDs 获取组织所有用户的ID列表
func (u *UserDao) GetOrgUserIDs(ctx context.Context, orgId string) ([]string, error) {
	filter := bson.M{"org_id": orgId}

	opts := options.Find().SetProjection(bson.M{"user_id": 1})

	users, err := mongoutil.Find[*User](ctx, u.Collection, filter, opts)
	if err != nil {
		return nil, err
	}

	userIDs := make([]string, 0, len(users))
	for _, user := range users {
		userIDs = append(userIDs, user.UserID)
	}

	return userIDs, nil
}

// VerifyUsersInOrg 批量验证用户是否属于指定组织
func (u *UserDao) VerifyUsersInOrg(ctx context.Context, userIDs []string, orgId string) ([]string, error) {
	filter := bson.M{
		"user_id": bson.M{"$in": userIDs},
		"org_id":  orgId,
	}

	opts := options.Find().SetProjection(bson.M{"user_id": 1})

	users, err := mongoutil.Find[*User](ctx, u.Collection, filter, opts)
	if err != nil {
		return nil, err
	}

	validUserIDs := make([]string, 0, len(users))
	for _, user := range users {
		validUserIDs = append(validUserIDs, user.UserID)
	}

	return validUserIDs, nil
}
func (u *UserDao) Find(ctx context.Context, userIDs []string) (users []*User, err error) {
	return mongoutil.Find[*User](ctx, u.Collection, bson.M{"user_id": bson.M{"$in": userIDs}}, options.Find().SetProjection(bson.M{"aes_key": 0}))
}
