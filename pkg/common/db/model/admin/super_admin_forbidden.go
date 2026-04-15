package admin

import (
	"context"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/openimsdk/chat/pkg/common/db/table/admin"
	"github.com/openimsdk/tools/errs"
)

func NewSuperAdminForbidden(db *mongo.Database) (admin.SuperAdminForbiddenInterface, error) {
	coll := db.Collection("super_admin_forbidden")
	_, err := coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{
			{Key: "user_id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &SuperAdminForbidden{
		coll: coll,
	}, nil
}

type SuperAdminForbidden struct {
	coll *mongo.Collection
}

func (o *SuperAdminForbidden) Take(ctx context.Context, userID string) (*admin.SuperAdminForbidden, error) {
	return mongoutil.FindOne[*admin.SuperAdminForbidden](ctx, o.coll, bson.M{"user_id": userID})
}
