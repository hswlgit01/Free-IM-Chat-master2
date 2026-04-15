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

func NewWithdrawalRule(db *mongo.Database) (chat.WithdrawalRuleInterface, error) {
	coll := db.Collection("withdrawal_rules")
	_, err := coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{
			{Key: "organization_id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &WithdrawalRule{coll: coll}, nil
}

type WithdrawalRule struct {
	coll *mongo.Collection
}

func (w *WithdrawalRule) Create(ctx context.Context, rule *chat.WithdrawalRule) error {
	if rule.ID.IsZero() {
		rule.ID = primitive.NewObjectID()
	}
	now := time.Now()
	rule.CreatedAt = now
	rule.UpdatedAt = now
	return mongoutil.InsertMany(ctx, w.coll, []*chat.WithdrawalRule{rule})
}

func (w *WithdrawalRule) FindByOrganizationID(ctx context.Context, organizationID string) (*chat.WithdrawalRule, error) {
	return mongoutil.FindOne[*chat.WithdrawalRule](
		ctx,
		w.coll,
		bson.M{"organization_id": organizationID},
	)
}

func (w *WithdrawalRule) Update(ctx context.Context, organizationID string, update map[string]any) error {
	if len(update) == 0 {
		return nil
	}
	update["updated_at"] = time.Now()
	return mongoutil.UpdateOne(
		ctx,
		w.coll,
		bson.M{"organization_id": organizationID},
		bson.M{"$set": update},
		true,
	)
}

func (w *WithdrawalRule) Upsert(ctx context.Context, rule *chat.WithdrawalRule) error {
	now := time.Now()
	rule.UpdatedAt = now

	update := bson.M{
		"$set": bson.M{
			"is_enabled":        rule.IsEnabled,
			"min_amount":        rule.MinAmount,
			"max_amount":        rule.MaxAmount,
			"fee_fixed":         rule.FeeFixed,
			"fee_rate":          rule.FeeRate,
			"need_real_name":    rule.NeedRealName,
			"need_bind_account": rule.NeedBindAccount,
			"updated_at":        now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := w.coll.UpdateOne(
		ctx,
		bson.M{"organization_id": rule.OrganizationID},
		update,
		opts,
	)
	return errs.Wrap(err)
}
