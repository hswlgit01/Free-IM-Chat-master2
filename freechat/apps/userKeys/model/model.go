package model

import (
	"context"
	"time"

	"github.com/openimsdk/chat/freechat/constant"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// UserKeys 用户密钥表
type UserKeys struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	UserID    string             `bson:"user_id" json:"user_id"`         // 用户ID
	WebAESKey string             `bson:"web_aes_key" json:"web_aes_key"` // Web端AES密钥
	IOSAESKey string             `bson:"ios_aes_key" json:"ios_aes_key"` // iOS端AES密钥
	H5AESKey  string             `bson:"h5_aes_key" json:"h5_aes_key"`   // H5端Aes密钥

	AndroidAESKey       string    `bson:"android_aes_key" json:"android_aes_key"`               // Android端AES密钥
	WebRSAPublicKey     string    `bson:"web_rsa_public_key" json:"web_rsa_public_key"`         // Web端RSA公钥
	IOSRSAPublicKey     string    `bson:"ios_rsa_public_key" json:"ios_rsa_public_key"`         // iOS端RSA公钥
	H5RSAPublicKey      string    `bson:"h5_rsa_public_key" json:"h5_rsa_public_key"`           // H5端RSA公钥
	AndroidRSAPublicKey string    `bson:"android_rsa_public_key" json:"android_rsa_public_key"` // Android端RSA公钥
	UpdatedAt           time.Time `bson:"updated_at" json:"updated_at"`                         // 更新时间
	CreatedAt           time.Time `bson:"created_at" json:"created_at"`                         // 创建时间
}

func (UserKeys) TableName() string {
	return constant.CollectionUserKeys
}

// CreateUserKeysIndexes 创建用户密钥表索引
func CreateUserKeysIndexes(db *mongo.Database) error {
	userKeys := &UserKeys{}

	// 创建索引
	_, err := db.Collection(userKeys.TableName()).Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "user_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "created_at", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "updated_at", Value: 1}},
		},
	})
	if err != nil {
		return errs.Wrap(err)
	}

	return nil
}

type UserKeysDao struct {
	DB         *mongo.Database
	Collection *mongo.Collection
}

func NewUserKeysDao(db *mongo.Database) *UserKeysDao {
	return &UserKeysDao{
		DB:         db,
		Collection: db.Collection(UserKeys{}.TableName()),
	}
}

// UpsertUserKeys 更新或插入用户密钥
func (d *UserKeysDao) UpsertUserKeys(ctx context.Context, userKeys *UserKeys) error {
	now := time.Now().UTC()
	userKeys.UpdatedAt = now

	filter := bson.M{"user_id": userKeys.UserID}
	update := bson.M{
		"$set": bson.M{
			"user_id":                userKeys.UserID,
			"web_aes_key":            userKeys.WebAESKey,
			"ios_aes_key":            userKeys.IOSAESKey,
			"h5_aes_key":             userKeys.H5AESKey,
			"android_aes_key":        userKeys.AndroidAESKey,
			"web_rsa_public_key":     userKeys.WebRSAPublicKey,
			"ios_rsa_public_key":     userKeys.IOSRSAPublicKey,
			"h5_rsa_public_key":      userKeys.H5RSAPublicKey,
			"android_rsa_public_key": userKeys.AndroidRSAPublicKey,
			"updated_at":             now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := d.Collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return errs.Wrap(err)
	}

	return nil
}

// GetUserKeysByUserID 根据用户ID获取用户密钥
func (d *UserKeysDao) GetUserKeysByUserID(ctx context.Context, userID string) (*UserKeys, error) {
	filter := bson.M{"user_id": userID}

	var userKeys UserKeys
	err := d.Collection.FindOne(ctx, filter).Decode(&userKeys)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return &userKeys, nil
}

// UpdateUserAESKey 更新用户特定平台的AES密钥
func (d *UserKeysDao) UpdateUserAESKey(ctx context.Context, userID, platform, aesKey string) error {
	now := time.Now().UTC()

	filter := bson.M{"user_id": userID}
	var update bson.M

	switch platform {
	case "web":
		update = bson.M{
			"$set": bson.M{
				"web_aes_key": aesKey,
				"updated_at":  now,
			},
			"$setOnInsert": bson.M{
				"created_at": now,
			},
		}
	case "ios":
		update = bson.M{
			"$set": bson.M{
				"ios_aes_key": aesKey,
				"updated_at":  now,
			},
			"$setOnInsert": bson.M{
				"created_at": now,
			},
		}
	case "android":
		update = bson.M{
			"$set": bson.M{
				"android_aes_key": aesKey,
				"updated_at":      now,
			},
			"$setOnInsert": bson.M{
				"created_at": now,
			},
		}
	case "h5":
		update = bson.M{
			"$set": bson.M{
				"h5_aes_key": aesKey,
				"updated_at": now,
			},
			"$setOnInsert": bson.M{
				"created_at": now,
			},
		}
	default:
		return errs.ErrArgs.WrapMsg("invalid platform")
	}

	opts := options.Update().SetUpsert(true)
	_, err := d.Collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return errs.Wrap(err)
	}

	return nil
}

// UpdateUserRSAPublicKey 更新用户特定平台的RSA公钥
func (d *UserKeysDao) UpdateUserRSAPublicKey(ctx context.Context, userID, platform, rsaPublicKey string) error {
	now := time.Now().UTC()

	filter := bson.M{"user_id": userID}
	var update bson.M

	switch platform {
	case "web":
		update = bson.M{
			"$set": bson.M{
				"web_rsa_public_key": rsaPublicKey,
				"updated_at":         now,
			},
			"$setOnInsert": bson.M{
				"created_at": now,
			},
		}
	case "ios":
		update = bson.M{
			"$set": bson.M{
				"ios_rsa_public_key": rsaPublicKey,
				"updated_at":         now,
			},
			"$setOnInsert": bson.M{
				"created_at": now,
			},
		}
	case "android":
		update = bson.M{
			"$set": bson.M{
				"android_rsa_public_key": rsaPublicKey,
				"updated_at":             now,
			},
			"$setOnInsert": bson.M{
				"created_at": now,
			},
		}
	case "h5":
		update = bson.M{
			"$set": bson.M{
				"h5_rsa_public_key": rsaPublicKey,
				"updated_at":        now,
			},
			"$setOnInsert": bson.M{
				"created_at": now,
			},
		}
	default:
		return errs.ErrArgs.WrapMsg("invalid platform")
	}

	opts := options.Update().SetUpsert(true)
	_, err := d.Collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return errs.Wrap(err)
	}

	return nil
}

// GetUserAESKey 获取用户特定平台的AES密钥
func (d *UserKeysDao) GetUserAESKey(ctx context.Context, userID, platform string) (string, error) {
	filter := bson.M{"user_id": userID}

	var projection bson.M
	switch platform {
	case "web":
		projection = bson.M{"web_aes_key": 1}
	case "ios":
		projection = bson.M{"ios_aes_key": 1}
	case "android":
		projection = bson.M{"android_aes_key": 1}
	case "h5":
		projection = bson.M{"h5_aes_key": 1}
	default:
		return "", errs.ErrArgs.WrapMsg("invalid platform")
	}

	opts := options.FindOne().SetProjection(projection)
	var result map[string]interface{}
	err := d.Collection.FindOne(ctx, filter, opts).Decode(&result)
	if err != nil {
		return "", errs.Wrap(err)
	}

	var aesKey string
	switch platform {
	case "web":
		aesKey, _ = result["web_aes_key"].(string)
	case "ios":
		aesKey, _ = result["ios_aes_key"].(string)
	case "android":
		aesKey, _ = result["android_aes_key"].(string)
	case "h5":
		aesKey, _ = result["h5_aes_key"].(string)
	}

	return aesKey, nil
}

// GetUserRSAPublicKey 获取用户特定平台的RSA公钥
func (d *UserKeysDao) GetUserRSAPublicKey(ctx context.Context, userID, platform string) (string, error) {
	filter := bson.M{"user_id": userID}

	var projection bson.M
	switch platform {
	case "web":
		projection = bson.M{"web_rsa_public_key": 1}
	case "ios":
		projection = bson.M{"ios_rsa_public_key": 1}
	case "android":
		projection = bson.M{"android_rsa_public_key": 1}
	case "h5":
		projection = bson.M{"h5_rsa_public_key": 1}
	default:
		return "", errs.ErrArgs.WrapMsg("invalid platform")
	}

	opts := options.FindOne().SetProjection(projection)
	var result map[string]interface{}
	err := d.Collection.FindOne(ctx, filter, opts).Decode(&result)
	if err != nil {
		return "", errs.Wrap(err)
	}

	var rsaPublicKey string
	switch platform {
	case "web":
		rsaPublicKey, _ = result["web_rsa_public_key"].(string)
	case "ios":
		rsaPublicKey, _ = result["ios_rsa_public_key"].(string)
	case "android":
		rsaPublicKey, _ = result["android_rsa_public_key"].(string)
	case "h5":
		rsaPublicKey, _ = result["h5_rsa_public_key"].(string)
	}

	return rsaPublicKey, nil
}
