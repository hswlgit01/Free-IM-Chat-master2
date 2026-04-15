package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// 抽奖活动

type Lottery struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrgId primitive.ObjectID `bson:"org_id" json:"org_id"`
	Name  string             `bson:"name" json:"name"`
	Desc  string             `bson:"desc" json:"desc"`

	ValidDays int `bson:"valid_days" json:"valid_days"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (Lottery) TableName() string {
	return constant.CollectionLottery
}

func CreateLotteryIndex(db *mongo.Database) error {
	m := &Lottery{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "created_at", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "org_id", Value: 1},
			},
		},
	})
	return err
}

type LotteryDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewLotteryDao(db *mongo.Database) *LotteryDao {
	return &LotteryDao{
		DB:         db,
		Collection: db.Collection(Lottery{}.TableName()),
	}
}

func (o *LotteryDao) Create(ctx context.Context, obj *Lottery) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*Lottery{obj})
}

func (o *LotteryDao) ExistByNameAndOrgId(ctx context.Context, name string, organizationId primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"name": name, "org_id": organizationId})
}

func (o *LotteryDao) GetByNameAndOrgId(ctx context.Context, name string, organizationId primitive.ObjectID) (*Lottery, error) {
	return mongoutil.FindOne[*Lottery](ctx, o.Collection, bson.M{"name": name, "org_id": organizationId})
}

func (o *LotteryDao) GetByIdAndOrgId(ctx context.Context, id primitive.ObjectID, organizationId primitive.ObjectID) (*Lottery, error) {
	return mongoutil.FindOne[*Lottery](ctx, o.Collection, bson.M{"_id": id, "org_id": organizationId})
}

func (o *LotteryDao) GetById(ctx context.Context, id primitive.ObjectID) (*Lottery, error) {
	return mongoutil.FindOne[*Lottery](ctx, o.Collection, bson.M{"_id": id})
}

type LotteryUpdateFieldParam struct {
	Name      string `bson:"name" json:"name"`
	Desc      string `bson:"desc" json:"desc"`
	ValidDays int    `bson:"valid_days" json:"valid_days"`
}

func (o *LotteryDao) UpdateById(ctx context.Context, id primitive.ObjectID, param *LotteryUpdateFieldParam) error {
	updateField := bson.M{"$set": bson.M{
		"valid_days": param.ValidDays,
		"name":       param.Name,
		"desc":       param.Desc,
		"updated_at": time.Now().UTC(),
	}}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": id}, updateField, false)
}

func (o *LotteryDao) Select(ctx context.Context, keyword string, orgId primitive.ObjectID, page *paginationUtils.DepPagination) (int64, []*Lottery, error) {
	filter := bson.M{}

	if !orgId.IsZero() {
		filter["org_id"] = orgId
	}

	if keyword != "" {
		filter["$or"] = []bson.M{
			{"name": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	opts := make([]*options.FindOptions, 0)
	// 默认排序
	opts = append(opts, options.Find().SetSort(bson.M{"created_at": -1}))

	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}

	data, err := mongoutil.Find[*Lottery](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}
