package sensitiveWord

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/chat/freechat/middleware"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/tools/apiresp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CollectionSensitiveWords = "sensitive_words"
	SensitiveWordEnabled     = int32(1)
	SensitiveWordDisabled    = int32(2)
)

var (
	ensureSensitiveWordIndexMu    sync.Mutex
	ensureSensitiveWordIndexReady bool
)

// dawn 2026-05-14 新增敏感词维护：后台按组织维护敏感词，消息服务读取同一集合做发送前替换。
type SensitiveWord struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	OrgID      primitive.ObjectID `bson:"org_id" json:"org_id"`
	OrgIDHex   string             `bson:"org_id_hex" json:"org_id_hex"`
	Word       string             `bson:"word" json:"word"`
	Status     int32              `bson:"status" json:"status"`
	Remark     string             `bson:"remark" json:"remark"`
	CreateTime time.Time          `bson:"create_time" json:"create_time"`
	UpdateTime time.Time          `bson:"update_time" json:"update_time"`
}

type SensitiveWordListResp struct {
	Total int64           `json:"total"`
	Data  []SensitiveWord `json:"data"`
}

type SaveSensitiveWordReq struct {
	ID     string `json:"id"`
	Word   string `json:"word" binding:"required"`
	Status int32  `json:"status"`
	Remark string `json:"remark"`
}

type DeleteSensitiveWordReq struct {
	IDs []string `json:"ids" binding:"required"`
}

type SensitiveWordCtl struct{}

func NewSensitiveWordCtl() *SensitiveWordCtl {
	return &SensitiveWordCtl{}
}

func ensureSensitiveWordIndexes(db *mongo.Database) error {
	ensureSensitiveWordIndexMu.Lock()
	defer ensureSensitiveWordIndexMu.Unlock()
	if ensureSensitiveWordIndexReady {
		return nil
	}
	coll := db.Collection(CollectionSensitiveWords)
	_, err := coll.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "org_id_hex", Value: 1}, {Key: "word", Value: 1}},
			Options: options.Index().SetName("sensitive_word_org_word").SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "org_id_hex", Value: 1}, {Key: "status", Value: 1}, {Key: "update_time", Value: -1}},
			Options: options.Index().SetName("sensitive_word_org_status_time"),
		},
	})
	if err != nil {
		return err
	}
	ensureSensitiveWordIndexReady = true
	return nil
}

func sensitiveWordCollection() (*mongo.Collection, error) {
	db := plugin.MongoCli().GetDB()
	if err := ensureSensitiveWordIndexes(db); err != nil {
		return nil, err
	}
	return db.Collection(CollectionSensitiveWords), nil
}

func normalizeWord(raw string) (string, error) {
	word := strings.TrimSpace(raw)
	if word == "" {
		return "", freeErrors.ParameterInvalidErr
	}
	if len([]rune(word)) > 100 {
		return "", freeErrors.ApiErr("敏感词长度不能超过100个字符")
	}
	return word, nil
}

func normalizeStatus(status int32) int32 {
	if status == SensitiveWordDisabled {
		return SensitiveWordDisabled
	}
	return SensitiveWordEnabled
}

// CmsList 查询敏感词维护列表。
func (ctl *SensitiveWordCtl) CmsList(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	page, err := paginationUtils.QueryToDepPagination(c)
	if err != nil {
		apiresp.GinError(c, freeErrors.PageParameterInvalidErr)
		return
	}
	status := int32(0)
	if raw := strings.TrimSpace(c.Query("status")); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			apiresp.GinError(c, freeErrors.ParameterInvalidErr)
			return
		}
		status = int32(v)
	}

	filter := bson.M{"org_id": org.ID}
	if status > 0 {
		filter["status"] = status
	}
	if keyword := strings.TrimSpace(c.Query("keyword")); keyword != "" {
		filter["word"] = bson.M{"$regex": regexp.QuoteMeta(keyword), "$options": "i"}
	}

	coll, err := sensitiveWordCollection()
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	total, err := coll.CountDocuments(c, filter)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	opts := page.ToOptions().SetSort(bson.D{{Key: "update_time", Value: -1}, {Key: "create_time", Value: -1}})
	cursor, err := coll.Find(c, filter, opts)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	defer cursor.Close(c)

	rows := make([]SensitiveWord, 0)
	if err := cursor.All(c, &rows); err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	apiresp.GinSuccess(c, SensitiveWordListResp{Total: total, Data: rows})
}

// CmsCreate 新增敏感词，重复词按当前组织覆盖为最新配置。
func (ctl *SensitiveWordCtl) CmsCreate(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	var req SaveSensitiveWordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	word, err := normalizeWord(req.Word)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	now := time.Now()
	coll, err := sensitiveWordCollection()
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	_, err = coll.UpdateOne(c,
		bson.M{"org_id_hex": org.ID.Hex(), "word": word},
		bson.M{
			"$set": bson.M{
				"org_id":      org.ID,
				"org_id_hex":  org.ID.Hex(),
				"word":        word,
				"status":      normalizeStatus(req.Status),
				"remark":      strings.TrimSpace(req.Remark),
				"update_time": now,
			},
			"$setOnInsert": bson.M{"create_time": now},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	apiresp.GinSuccess(c, map[string]any{})
}

// CmsUpdate 修改敏感词。
func (ctl *SensitiveWordCtl) CmsUpdate(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	var req SaveSensitiveWordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	id, err := primitive.ObjectIDFromHex(strings.TrimSpace(req.ID))
	if err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	word, err := normalizeWord(req.Word)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	coll, err := sensitiveWordCollection()
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	result, err := coll.UpdateOne(c,
		bson.M{"_id": id, "org_id": org.ID},
		bson.M{"$set": bson.M{
			"word":        word,
			"status":      normalizeStatus(req.Status),
			"remark":      strings.TrimSpace(req.Remark),
			"update_time": time.Now(),
		}},
	)
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	if result.MatchedCount == 0 {
		apiresp.GinError(c, freeErrors.NotFoundErrWithResource("sensitive word"))
		return
	}
	apiresp.GinSuccess(c, map[string]any{})
}

// CmsDelete 删除敏感词。
func (ctl *SensitiveWordCtl) CmsDelete(c *gin.Context) {
	org, err := middleware.GetOrgInfoFromCtx(c)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	var req DeleteSensitiveWordReq
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	ids := make([]primitive.ObjectID, 0, len(req.IDs))
	for _, raw := range req.IDs {
		id, err := primitive.ObjectIDFromHex(strings.TrimSpace(raw))
		if err != nil {
			apiresp.GinError(c, freeErrors.ParameterInvalidErr)
			return
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		apiresp.GinError(c, freeErrors.ParameterInvalidErr)
		return
	}
	coll, err := sensitiveWordCollection()
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	_, err = coll.DeleteMany(c, bson.M{"_id": bson.M{"$in": ids}, "org_id": org.ID})
	if err != nil {
		apiresp.GinError(c, freeErrors.SystemErr(err))
		return
	}
	apiresp.GinSuccess(c, map[string]any{})
}
