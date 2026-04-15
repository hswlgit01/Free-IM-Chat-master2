// Copyright © 2023 OpenIM open source community. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chat

import (
	"context"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/openimsdk/chat/tools/db/mongoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/openimsdk/chat/pkg/common/db/table/chat"
)

func NewOrganizationUser(db *mongo.Database) (chat.OrganizationUserInterface, error) {
	coll := db.Collection("organization_user")
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "im_server_user_id", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "organization_id", Value: 1},
			},
		},
		// Hierarchy-related indexes
		{
			Keys: bson.D{
				{Key: "invitation_code", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "inviter", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "ancestor_path", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "level1_parent", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "level2_parent", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "level3_parent", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "level", Value: 1},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &OrganizationUser{coll: coll}, nil
}

type OrganizationUser struct {
	coll *mongo.Collection
}

func (o *OrganizationUser) Create(ctx context.Context, organizationUsers ...*chat.OrganizationUser) error {
	return mongoutil.InsertMany(ctx, o.coll, organizationUsers)
}

func (o *OrganizationUser) Find(ctx context.Context, userIds []string) ([]*chat.OrganizationUser, error) {
	return mongoutil.Find[*chat.OrganizationUser](ctx, o.coll, bson.M{"user_id": bson.M{"$in": userIds}})
}

func (o *OrganizationUser) FindByImServerUserIds(ctx context.Context, imServerUserIds []string) ([]*chat.OrganizationUser, error) {
	return mongoutil.Find[*chat.OrganizationUser](ctx, o.coll, bson.M{"im_server_user_id": bson.M{"$in": imServerUserIds}})
}

func (o *OrganizationUser) Take(ctx context.Context, userID string) (*chat.OrganizationUser, error) {
	return mongoutil.FindOne[*chat.OrganizationUser](ctx, o.coll, bson.M{"user_id": userID})
}

func (o *OrganizationUser) TakeByOrgid(ctx context.Context, userID, orgid string) (*chat.OrganizationUser, error) {
	organizationObjectID, _ := primitive.ObjectIDFromHex(orgid)
	return mongoutil.FindOne[*chat.OrganizationUser](ctx, o.coll, bson.M{"user_id": userID, "organization_id": organizationObjectID})
}

func (o *OrganizationUser) Update(ctx context.Context, userID string, data map[string]any) error {
	return mongoutil.UpdateOne(ctx, o.coll, bson.M{"user_id": userID}, bson.M{"$set": data}, false)
}

func (o *OrganizationUser) Delete(ctx context.Context, userIDs []string) error {
	return mongoutil.DeleteMany(ctx, o.coll, bson.M{"user_id": bson.M{"$in": userIDs}})
}

// Hierarchy-related methods implementation

// FindByInvitationCode finds a user by their invitation code
func (o *OrganizationUser) FindByInvitationCode(ctx context.Context, invitationCode string) (*chat.OrganizationUser, error) {
	return mongoutil.FindOne[*chat.OrganizationUser](ctx, o.coll, bson.M{"invitation_code": invitationCode})
}

// FindDirectDownline finds users who have the given user as their level1_parent
func (o *OrganizationUser) FindDirectDownline(ctx context.Context, userID string, page, size int64) ([]*chat.OrganizationUser, int64, error) {
	skip := (page - 1) * size
	filter := bson.M{"level1_parent": userID}

	count, err := o.coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	opts := mongoutil.FindOptions(skip, size, bson.D{{Key: "created_at", Value: -1}})
	cursor, err := o.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}

	var results []*chat.OrganizationUser
	if err := cursor.All(ctx, &results); err != nil {
		return nil, 0, err
	}

	return results, count, nil
}

// FindByAncestor finds users who have the given user ID in their ancestor_path
func (o *OrganizationUser) FindByAncestor(ctx context.Context, ancestorID string, page, size int64) ([]*chat.OrganizationUser, int64, error) {
	skip := (page - 1) * size
	filter := bson.M{"ancestor_path": ancestorID}

	count, err := o.coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	opts := mongoutil.FindOptions(skip, size, bson.D{{Key: "level", Value: 1}, {Key: "created_at", Value: -1}})
	cursor, err := o.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}

	var results []*chat.OrganizationUser
	if err := cursor.All(ctx, &results); err != nil {
		return nil, 0, err
	}

	return results, count, nil
}

// UpdateHierarchy updates hierarchy-specific fields for a user
func (o *OrganizationUser) UpdateHierarchy(ctx context.Context, userID string, hierarchyData map[string]any) error {
	return mongoutil.UpdateOne(ctx, o.coll, bson.M{"user_id": userID}, bson.M{"$set": hierarchyData}, false)
}

// IncrementTeamSize increments the team_size field for multiple users
func (o *OrganizationUser) IncrementTeamSize(ctx context.Context, userIDs []string, increment int) error {
	_, err := mongoutil.UpdateMany(ctx, o.coll, bson.M{"user_id": bson.M{"$in": userIDs}}, bson.M{"$inc": bson.M{"team_size": increment}})
	return err
}

// FindByLevel finds users by their hierarchy level
func (o *OrganizationUser) FindByLevel(ctx context.Context, level int, page, size int64) ([]*chat.OrganizationUser, int64, error) {
	skip := (page - 1) * size
	filter := bson.M{"level": level}

	count, err := o.coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	opts := mongoutil.FindOptions(skip, size, bson.D{{Key: "created_at", Value: -1}})
	cursor, err := o.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}

	var results []*chat.OrganizationUser
	if err := cursor.All(ctx, &results); err != nil {
		return nil, 0, err
	}

	return results, count, nil
}
