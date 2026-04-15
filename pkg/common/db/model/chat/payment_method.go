package chat

import (
	"context"
	"time"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"
	"github.com/openimsdk/chat/tools/db/mongoutil"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewPaymentMethod(db *mongo.Database) (chat.PaymentMethodInterface, error) {
	coll := db.Collection("payment_methods")
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
				{Key: "is_default", Value: -1},
			},
		},
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
				{Key: "created_at", Value: -1},
			},
		},
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &PaymentMethod{coll: coll}, nil
}

type PaymentMethod struct {
	coll *mongo.Collection
}

func (p *PaymentMethod) Create(ctx context.Context, paymentMethod *chat.PaymentMethod) error {
	if paymentMethod.ID.IsZero() {
		paymentMethod.ID = primitive.NewObjectID()
	}
	now := time.Now()
	paymentMethod.CreatedAt = now
	paymentMethod.UpdatedAt = now
	return mongoutil.InsertMany(ctx, p.coll, []*chat.PaymentMethod{paymentMethod})
}

func (p *PaymentMethod) FindByUserID(ctx context.Context, userID string) ([]*chat.PaymentMethod, error) {
	return mongoutil.Find[*chat.PaymentMethod](
		ctx,
		p.coll,
		bson.M{"user_id": userID},
		options.Find().SetSort(bson.D{{Key: "is_default", Value: -1}, {Key: "created_at", Value: -1}}),
	)
}

func (p *PaymentMethod) FindByID(ctx context.Context, id primitive.ObjectID) (*chat.PaymentMethod, error) {
	return mongoutil.FindOne[*chat.PaymentMethod](ctx, p.coll, bson.M{"_id": id})
}

func (p *PaymentMethod) Update(ctx context.Context, id primitive.ObjectID, update map[string]any) error {
	if len(update) == 0 {
		return nil
	}
	update["updated_at"] = time.Now()
	return mongoutil.UpdateOne(ctx, p.coll, bson.M{"_id": id}, bson.M{"$set": update}, true)
}

func (p *PaymentMethod) Delete(ctx context.Context, id primitive.ObjectID) error {
	return mongoutil.DeleteOne(ctx, p.coll, bson.M{"_id": id})
}

func (p *PaymentMethod) SetDefault(ctx context.Context, userID string, id primitive.ObjectID) error {
	// 使用事务确保原子性
	session, err := p.coll.Database().Client().StartSession()
	if err != nil {
		return errs.Wrap(err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// 1. 取消该用户的其他默认支付方式
		_, err := p.coll.UpdateMany(
			sessCtx,
			bson.M{"user_id": userID, "is_default": true},
			bson.M{"$set": bson.M{"is_default": false, "updated_at": time.Now()}},
		)
		if err != nil {
			return nil, errs.Wrap(err)
		}

		// 2. 设置新的默认支付方式
		result := p.coll.FindOneAndUpdate(
			sessCtx,
			bson.M{"_id": id, "user_id": userID},
			bson.M{"$set": bson.M{"is_default": true, "updated_at": time.Now()}},
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		)
		if result.Err() != nil {
			if result.Err() == mongo.ErrNoDocuments {
				return nil, errs.ErrRecordNotFound.WrapMsg("payment method not found")
			}
			return nil, errs.Wrap(result.Err())
		}

		return nil, nil
	})

	return err
}

func (p *PaymentMethod) FindDefaultByUserID(ctx context.Context, userID string) (*chat.PaymentMethod, error) {
	return mongoutil.FindOne[*chat.PaymentMethod](
		ctx,
		p.coll,
		bson.M{"user_id": userID, "is_default": true},
	)
}
