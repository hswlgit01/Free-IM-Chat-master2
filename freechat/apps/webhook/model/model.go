package model

import (
	"context"
	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

//type WebhookStatus string
//
//const (
//	WebhookStatusEnable  WebhookStatus = "enable"
//	WebhookStatusDisable WebhookStatus = "disable"
//)

type Webhook struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrganizationId primitive.ObjectID `bson:"organization_id" json:"organization_id"` // 组织ID
	Url            string             `bson:"url" json:"url"`
	Status         bool               `bson:"status" json:"status"`
	CreatorId      string             `bson:"creator_id" json:"creator_id"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func (Webhook) TableName() string {
	return constant.CollectionWebhook
}

func CreateWebhookIndex(db *mongo.Database) error {
	m := &Webhook{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "creator_id", Value: 1},
			},
		},
	})
	return err
}

type WebhookDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewWebhookDao(db *mongo.Database) *WebhookDao {
	return &WebhookDao{
		DB:         db,
		Collection: db.Collection(Webhook{}.TableName()),
	}
}

func (o *WebhookDao) Create(ctx context.Context, obj *Webhook) error {
	obj.UpdatedAt = time.Now().UTC()
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*Webhook{obj})
}

type WebhookUpdateFieldParam struct {
	Url    string `bson:"url" json:"url"`
	Status bool   `bson:"status" json:"status"`
}

func (o *WebhookDao) UpdateById(ctx context.Context, id primitive.ObjectID, param *WebhookUpdateFieldParam) error {
	updateField := bson.M{"$set": bson.M{
		"status":     param.Status,
		"url":        param.Url,
		"updated_at": time.Now().UTC(),
	}}
	return mongoutil.UpdateOne(ctx, o.Collection, bson.M{"_id": id}, updateField, false)
}

func (o *WebhookDao) GetById(ctx context.Context, id primitive.ObjectID) (*Webhook, error) {
	return mongoutil.FindOne[*Webhook](ctx, o.Collection, bson.M{"_id": id})
}

func (o *WebhookDao) GetByUrlAndOrganizationId(ctx context.Context, url string, organizationId primitive.ObjectID) (*Webhook, error) {
	return mongoutil.FindOne[*Webhook](ctx, o.Collection, bson.M{"url": url, "organization_id": organizationId})
}

func (o *WebhookDao) ExistByUrlAndOrganizationId(ctx context.Context, url string, organizationId primitive.ObjectID) (bool, error) {
	return mongoutil.Exist(ctx, o.Collection, bson.M{"url": url, "organization_id": organizationId})
}

func (o *WebhookDao) DeleteById(ctx context.Context, id primitive.ObjectID) error {
	filter := bson.M{}
	filter["_id"] = id
	return mongoutil.DeleteMany(ctx, o.Collection, filter)
}

func (o *WebhookDao) Select(ctx context.Context, keyword string, organizationId primitive.ObjectID, status *bool,
	page *paginationUtils.DepPagination) (int64, []*Webhook, error) {
	filter := bson.M{}

	if !organizationId.IsZero() {
		filter["organization_id"] = organizationId
	}

	// 添加关键词搜索
	if keyword != "" {
		filter["$or"] = []bson.M{
			{"url": bson.M{"$regex": keyword, "$options": "i"}},
		}
	}

	if status != nil {
		filter["status"] = status
	}

	opts := make([]*options.FindOptions, 0)
	if page != nil {
		opts = append(opts, page.ToOptions())
	}

	if len(filter) == 0 {
		filter = nil
	}
	log.ZInfo(ctx, "Select", "filter", filter, "page", page)

	data, err := mongoutil.Find[*Webhook](ctx, o.Collection, filter, opts...)
	if err != nil {
		return 0, nil, err
	}

	total, err := mongoutil.Count(ctx, o.Collection, filter)
	if err != nil {
		return 0, nil, err
	}
	return total, data, nil
}

func (o *WebhookDao) SelectJoinWebhookTrigger(ctx context.Context, organizationId primitive.ObjectID, status *bool,
	event WebhookTriggerEvent) (int64, []*Webhook, error) {
	filter := bson.M{}

	// 聚合查询
	pipeline := []bson.M{
		{
			"$lookup": bson.M{
				"from":         WebhookTrigger{}.TableName(),
				"localField":   "_id",
				"foreignField": "webhook_id",
				"as":           "webhook_trigger",
			},
		},
	}

	// 构建过滤条件
	if !organizationId.IsZero() {
		filter["organization_id"] = organizationId
	}

	if status != nil {
		filter["status"] = status
	}

	if event != "" {
		filter["webhook_trigger.event"] = event
	}

	findPipeline := make([]bson.M, 0)
	countPipeline := make([]bson.M, 0)

	if len(filter) > 0 {
		findPipeline = append(pipeline, bson.M{"$match": filter})
		countPipeline = append(pipeline, bson.M{"$match": filter})
	}

	findPipeline = append(findPipeline, bson.M{"$sort": bson.M{"created_at": 1}})

	// 执行聚合查询获取数据
	data, err := mongoutil.Aggregate[*Webhook](ctx, o.Collection, findPipeline)
	if err != nil {
		return 0, nil, err
	}

	// 查询总数
	countPipeline = append(countPipeline, bson.M{"$count": "total"})
	cursor, err := o.Collection.Aggregate(ctx, countPipeline)
	if err != nil {
		return 0, nil, err
	}
	var countResult []bson.M
	if err = cursor.All(ctx, &countResult); err != nil {
		return 0, nil, err
	}

	total := int32(0)
	if len(countResult) > 0 {
		total = countResult[0]["total"].(int32)
	}

	return int64(total), data, nil
}

type WebhookTriggerEvent string

const (
	WebhookTriggerTransferEvent WebhookTriggerEvent = "transfer"
	WebhookTriggerRechargeEvent WebhookTriggerEvent = "recharge"
)

var AllWebhookTriggerEvent = []WebhookTriggerEvent{
	WebhookTriggerTransferEvent,
	WebhookTriggerRechargeEvent,
}

type WebhookTrigger struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	WebhookId primitive.ObjectID  `bson:"webhook_id" json:"webhook_id"`
	Event     WebhookTriggerEvent `bson:"event" json:"event"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

func (WebhookTrigger) TableName() string {
	return constant.CollectionWebhookTrigger
}

func CreateWebhookTriggerIndex(db *mongo.Database) error {
	m := &WebhookTrigger{}

	coll := db.Collection(m.TableName())
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "webhook_id", Value: 1},
				{Key: "event", Value: 1},
			},
		},
	})
	return err
}

type WebhookTriggerDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewWebhookTriggerDao(db *mongo.Database) *WebhookTriggerDao {
	return &WebhookTriggerDao{
		DB:         db,
		Collection: db.Collection(WebhookTrigger{}.TableName()),
	}
}

func (o *WebhookTriggerDao) GetByWebhookId(ctx context.Context, webhookId primitive.ObjectID) ([]*WebhookTrigger, error) {
	filter := bson.M{}
	filter["webhook_id"] = webhookId
	return mongoutil.Find[*WebhookTrigger](ctx, o.Collection, filter)
}

func (o *WebhookTriggerDao) DeleteByWebhookId(ctx context.Context, webhookId primitive.ObjectID) error {
	filter := bson.M{}
	filter["webhook_id"] = webhookId
	return mongoutil.DeleteMany(ctx, o.Collection, filter)
}

func (o *WebhookTriggerDao) Create(ctx context.Context, obj *WebhookTrigger) error {
	obj.CreatedAt = time.Now().UTC()
	return mongoutil.InsertMany(ctx, o.Collection, []*WebhookTrigger{obj})
}
