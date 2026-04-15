package dto

import (
	"context"
	"github.com/openimsdk/chat/freechat/apps/webhook/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type WebhookResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	OrganizationId primitive.ObjectID `bson:"organization_id" json:"organization_id"` // 组织ID
	Url            string             `bson:"url" json:"url"`
	Status         bool               `bson:"status" json:"status"`
	CreatorId      string             `bson:"creator_id" json:"creator_id"`

	WebhookTriggerEvent []*WebhookTriggerResp `bson:"webhook_trigger_event" json:"webhook_trigger_event"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func NewWebhookResp(db *mongo.Database, webhook *model.Webhook) (*WebhookResp, error) {
	obj := &WebhookResp{
		ID:             webhook.ID,
		OrganizationId: webhook.OrganizationId,
		Url:            webhook.Url,
		Status:         webhook.Status,
		CreatorId:      webhook.CreatorId,
		CreatedAt:      webhook.CreatedAt,
		UpdatedAt:      webhook.UpdatedAt,
	}

	webhookTriggerDao := model.NewWebhookTriggerDao(db)
	webhookTrigger, err := webhookTriggerDao.GetByWebhookId(context.TODO(), webhook.ID)
	if err != nil {
		return nil, err
	}

	for _, trigger := range webhookTrigger {
		obj.WebhookTriggerEvent = append(obj.WebhookTriggerEvent, NewWebhookTriggerResp(trigger))
	}

	return obj, nil
}

type WebhookTriggerResp struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`

	WebhookId primitive.ObjectID        `bson:"webhook_id" json:"webhook_id"`
	Event     model.WebhookTriggerEvent `bson:"event" json:"event"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

func NewWebhookTriggerResp(webhookTrigger *model.WebhookTrigger) *WebhookTriggerResp {
	obj := &WebhookTriggerResp{
		ID:        webhookTrigger.ID,
		WebhookId: webhookTrigger.WebhookId,
		Event:     webhookTrigger.Event,
		CreatedAt: webhookTrigger.CreatedAt,
	}
	return obj
}
