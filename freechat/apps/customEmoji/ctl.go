package customEmoji

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/tools/apiresp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CollectionCustomEmojis = "custom_emojis"
	maxCustomEmojiCount    = 200
	maxResourceURLLength   = 2048
)

var (
	ensureCustomEmojiIndexMu    sync.Mutex
	ensureCustomEmojiIndexReady bool
)

// dawn 2026-06-04 新增收藏图片服务端持久化：用户收藏图片从本地缓存升级为按组织和用户保存到服务端。
type CustomEmoji struct {
	ID             primitive.ObjectID `bson:"_id,omitempty"`
	OrgID          primitive.ObjectID `bson:"org_id" json:"org_id"`
	OrgIDHex       string             `bson:"org_id_hex" json:"org_id_hex"`
	UserID         string             `bson:"user_id" json:"user_id"`
	ImServerUserID string             `bson:"im_server_user_id" json:"im_server_user_id"`
	URL            string             `bson:"url" json:"url"`
	Path           string             `bson:"path" json:"path"`
	Width          int                `bson:"width" json:"width"`
	Height         int                `bson:"height" json:"height"`
	Sort           int64              `bson:"sort" json:"sort"`
	CreateTime     time.Time          `bson:"create_time" json:"create_time"`
	UpdateTime     time.Time          `bson:"update_time" json:"update_time"`
}

type CustomEmojiResp struct {
	ID             string    `json:"id"`
	OrgID          string    `json:"org_id"`
	UserID         string    `json:"user_id"`
	ImServerUserID string    `json:"im_server_user_id"`
	URL            string    `json:"url"`
	Path           string    `json:"path"`
	Width          int       `json:"width"`
	Height         int       `json:"height"`
	Sort           int64     `json:"sort"`
	CreateTime     time.Time `json:"create_time"`
	UpdateTime     time.Time `json:"update_time"`
}

type ListResp struct {
	Items []CustomEmojiResp `json:"items"`
}

type ItemResp struct {
	Item CustomEmojiResp `json:"item"`
}

type CustomEmojiItemReq struct {
	ID     string `json:"id"`
	URL    string `json:"url" binding:"required"`
	Path   string `json:"path"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

type SaveReq struct {
	Items []CustomEmojiItemReq `json:"items"`
}

type DeleteReq struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type CustomEmojiCtl struct{}

func NewCustomEmojiCtl() *CustomEmojiCtl {
	return &CustomEmojiCtl{}
}

func ensureCustomEmojiIndexes(db *mongo.Database) error {
	ensureCustomEmojiIndexMu.Lock()
	defer ensureCustomEmojiIndexMu.Unlock()
	if ensureCustomEmojiIndexReady {
		return nil
	}
	coll := db.Collection(CollectionCustomEmojis)
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "org_id_hex", Value: 1}, {Key: "im_server_user_id", Value: 1}, {Key: "url", Value: 1}},
			Options: options.Index().SetName("custom_emoji_owner_url").SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "org_id_hex", Value: 1}, {Key: "im_server_user_id", Value: 1}, {Key: "sort", Value: 1}},
			Options: options.Index().SetName("custom_emoji_owner_sort"),
		},
	})
	if err != nil {
		return err
	}
	ensureCustomEmojiIndexReady = true
	return nil
}

func customEmojiCollection() (*mongo.Collection, error) {
	db := plugin.MongoCli().GetDB()
	if err := ensureCustomEmojiIndexes(db); err != nil {
		return nil, err
	}
	return db.Collection(CollectionCustomEmojis), nil
}

func ownerFilter(org *middleware.OrgInfo) bson.M {
	return bson.M{
		"org_id_hex":        org.ID.Hex(),
		"im_server_user_id": org.OrgUser.ImServerUserId,
	}
}

func normalizeItem(raw CustomEmojiItemReq) (CustomEmojiItemReq, error) {
	item := CustomEmojiItemReq{
		ID:     strings.TrimSpace(raw.ID),
		URL:    strings.TrimSpace(raw.URL),
		Path:   strings.TrimSpace(raw.Path),
		Width:  raw.Width,
		Height: raw.Height,
	}
	if item.URL == "" {
		return item, freeErrors.ParameterInvalidErr
	}
	if len(item.URL) > maxResourceURLLength {
		return item, freeErrors.ApiErr("收藏图片地址过长")
	}
	if item.Width < 0 || item.Height < 0 {
		return item, freeErrors.ParameterInvalidErr
	}
	return item, nil
}

func toResp(row CustomEmoji) CustomEmojiResp {
	return CustomEmojiResp{
		ID:             row.ID.Hex(),
		OrgID:          row.OrgID.Hex(),
		UserID:         row.UserID,
		ImServerUserID: row.ImServerUserID,
		URL:            row.URL,
		Path:           row.Path,
		Width:          row.Width,
		Height:         row.Height,
		Sort:           row.Sort,
		CreateTime:     row.CreateTime,
		UpdateTime:     row.UpdateTime,
	}
}

func getOrgInfo(c *gin.Context) (*middleware.OrgInfo, bool) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return nil, false
	}
	return org, true
}

// List 查询当前用户收藏图片列表。
func (ctl *CustomEmojiCtl) List(c *gin.Context) {
	org, ok := getOrgInfo(c)
	if !ok {
		return
	}
	coll, err := customEmojiCollection()
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	cursor, err := coll.Find(c, ownerFilter(org), options.Find().
		SetSort(bson.D{{Key: "sort", Value: 1}, {Key: "create_time", Value: 1}}).
		SetLimit(maxCustomEmojiCount))
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	defer cursor.Close(c)

	rows := make([]CustomEmoji, 0)
	if err := cursor.All(c, &rows); err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	items := make([]CustomEmojiResp, 0, len(rows))
	for _, row := range rows {
		items = append(items, toResp(row))
	}
	apiresp.GinSuccess(c, ListResp{Items: items})
}

// Add 新增或覆盖当前用户的一条收藏图片。
func (ctl *CustomEmojiCtl) Add(c *gin.Context) {
	org, ok := getOrgInfo(c)
	if !ok {
		return
	}
	var req CustomEmojiItemReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	item, err := normalizeItem(req)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	coll, err := customEmojiCollection()
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	count, err := coll.CountDocuments(c, ownerFilter(org))
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	filter := ownerFilter(org)
	filter["url"] = item.URL
	if count >= maxCustomEmojiCount {
		existingCount, err := coll.CountDocuments(c, filter)
		if err != nil {
			apiresp.GinError(c, freeErrors.SystemErr(err))
			return
		}
		if existingCount == 0 {
			apiresp.GinError(c, freeErrors.ApiErr("收藏图片数量不能超过200个"))
			return
		}
	}
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"path":        item.Path,
			"width":       item.Width,
			"height":      item.Height,
			"update_time": now,
		},
		"$setOnInsert": bson.M{
			"org_id":            org.ID,
			"org_id_hex":        org.ID.Hex(),
			"user_id":           org.OrgUser.UserId,
			"im_server_user_id": org.OrgUser.ImServerUserId,
			"url":               item.URL,
			"sort":              count,
			"create_time":       now,
		},
	}
	var row CustomEmoji
	err = coll.FindOneAndUpdate(c, filter, update, options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)).Decode(&row)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	apiresp.GinSuccess(c, ItemResp{Item: toResp(row)})
}

// Save 覆盖保存当前用户的收藏图片列表，用于本地旧数据迁移、删除和排序同步。
func (ctl *CustomEmojiCtl) Save(c *gin.Context) {
	org, ok := getOrgInfo(c)
	if !ok {
		return
	}
	var req SaveReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	if len(req.Items) > maxCustomEmojiCount {
		apiresp.GinError(c, freeErrors.ApiErr("收藏图片数量不能超过200个"))
		return
	}
	seen := make(map[string]struct{}, len(req.Items))
	items := make([]CustomEmojiItemReq, 0, len(req.Items))
	for _, raw := range req.Items {
		item, err := normalizeItem(raw)
		if err != nil {
			apiresp.GinError(c, err)
			return
		}
		if _, exists := seen[item.URL]; exists {
			continue
		}
		seen[item.URL] = struct{}{}
		items = append(items, item)
	}
	coll, err := customEmojiCollection()
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	filter := ownerFilter(org)
	if _, err := coll.DeleteMany(c, filter); err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	if len(items) == 0 {
		apiresp.GinSuccess(c, ListResp{Items: []CustomEmojiResp{}})
		return
	}
	now := time.Now()
	docs := make([]interface{}, 0, len(items))
	for idx, item := range items {
		docs = append(docs, CustomEmoji{
			ID:             primitive.NewObjectID(),
			OrgID:          org.ID,
			OrgIDHex:       org.ID.Hex(),
			UserID:         org.OrgUser.UserId,
			ImServerUserID: org.OrgUser.ImServerUserId,
			URL:            item.URL,
			Path:           item.Path,
			Width:          item.Width,
			Height:         item.Height,
			Sort:           int64(idx),
			CreateTime:     now,
			UpdateTime:     now,
		})
	}
	if _, err := coll.InsertMany(c, docs); err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	ctl.List(c)
}

// Delete 删除当前用户的一条收藏图片。
func (ctl *CustomEmojiCtl) Delete(c *gin.Context) {
	org, ok := getOrgInfo(c)
	if !ok {
		return
	}
	var req DeleteReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	filter := ownerFilter(org)
	id := strings.TrimSpace(req.ID)
	url := strings.TrimSpace(req.URL)
	if id != "" {
		objectID, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			apiresp.GinError(c, freeErrors.ParameterInvalidErr)
			return
		}
		filter["_id"] = objectID
	} else if url != "" {
		filter["url"] = url
	} else {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	coll, err := customEmojiCollection()
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	if _, err := coll.DeleteOne(c, filter); err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	apiresp.GinSuccess(c, map[string]any{})
}
