package svc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openimsdk/chat/freechat/apps/webhook/dto"
	"github.com/openimsdk/chat/freechat/apps/webhook/model"
	"github.com/openimsdk/chat/freechat/plugin"
	"github.com/openimsdk/chat/freechat/utils/freeErrors"
	"github.com/openimsdk/chat/freechat/utils/paginationUtils"
	"github.com/openimsdk/chat/pkg/common/db/dbutil"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"net/http"
	"regexp"
	"slices"
)

type WebhookEventSvc struct {
}

func NewWebhookEventSvc() *WebhookEventSvc {
	return &WebhookEventSvc{}
}

func (s WebhookEventSvc) ListWebhookEvent() []model.WebhookTriggerEvent {
	return []model.WebhookTriggerEvent{
		model.WebhookTriggerTransferEvent,
		model.WebhookTriggerRechargeEvent,
	}
}

type OrgWebhookSvc struct {
}

func NewOrgWebhookSvc() *OrgWebhookSvc {
	return &OrgWebhookSvc{}
}

type CreateWebhookReq struct {
	Url          string   `json:"url" binding:"required"`
	WebhookEvent []string `json:"webhook_event"`
}

func (c *CreateWebhookReq) validateUrl() error {
	urlPattern := `^(https?):\/\/`
	re := regexp.MustCompile(urlPattern)
	if !re.MatchString(c.Url) {
		return fmt.Errorf("URL '%s' 必须要以http或https开头", c.Url)
	}
	return nil
}

func (s *OrgWebhookSvc) CreateWebhook(ctx context.Context, req CreateWebhookReq, orgId primitive.ObjectID) error {
	db := plugin.MongoCli().GetDB()
	webhookDao := model.NewWebhookDao(db)
	webhookEventDao := model.NewWebhookTriggerDao(db)

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		if err := req.validateUrl(); err != nil {
			return err
		}

		exist, err := webhookDao.ExistByUrlAndOrganizationId(sessionCtx, req.Url, orgId)
		if err != nil {
			return err
		}
		if exist {
			return errors.New("webhook url already exists, url" + req.Url)
		}

		webhook := &model.Webhook{
			OrganizationId: orgId,
			Url:            req.Url,
			Status:         true,
		}

		err = webhookDao.Create(sessionCtx, webhook)
		if err != nil {
			return err
		}

		newWebhook, err := webhookDao.GetByUrlAndOrganizationId(sessionCtx, webhook.Url, orgId)
		if err != nil {
			return err
		}

		for _, event := range req.WebhookEvent {
			e := model.WebhookTriggerEvent(event)
			if !slices.Contains(model.AllWebhookTriggerEvent, e) {
				return fmt.Errorf("invalid webhook event, %s", e)
			}
			webhookEvent := &model.WebhookTrigger{
				WebhookId: newWebhook.ID,
				Event:     model.WebhookTriggerEvent(event),
			}
			err = webhookEventDao.Create(sessionCtx, webhookEvent)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return errs.Unwrap(err)
}

type UpdateWebhookReq struct {
	WebhookId    primitive.ObjectID `json:"webhook_id" binding:"required"`
	Url          string             `json:"url" binding:"required"`
	WebhookEvent []string           `json:"webhook_event"`
	Status       bool               `json:"status"`
}

func (c *UpdateWebhookReq) validateUrl() error {
	urlPattern := `^(https?):\/\/`
	re := regexp.MustCompile(urlPattern)
	if !re.MatchString(c.Url) {
		return fmt.Errorf("URL '%s' 必须要以http或https开头", c.Url)
	}
	return nil
}

func (s *OrgWebhookSvc) UpdateWebhook(ctx context.Context, req UpdateWebhookReq, orgId primitive.ObjectID) error {
	db := plugin.MongoCli().GetDB()
	webhookDao := model.NewWebhookDao(db)
	webhookEventDao := model.NewWebhookTriggerDao(db)

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		if err := req.validateUrl(); err != nil {
			return err
		}

		webhook, err := webhookDao.GetById(sessionCtx, req.WebhookId)
		if err != nil {
			return err
		}

		// 如果url改变,重新校验url是否重复设置了
		if webhook.Url != req.Url {
			exist, err := webhookDao.ExistByUrlAndOrganizationId(sessionCtx, req.Url, orgId)
			if err != nil {
				return err
			}
			if exist {
				return errors.New("webhook url already exists, url" + req.Url)
			}
		}

		updateField := &model.WebhookUpdateFieldParam{
			Url:    req.Url,
			Status: req.Status,
		}
		err = webhookDao.UpdateById(sessionCtx, req.WebhookId, updateField)
		if err != nil {
			return err
		}

		err = webhookEventDao.DeleteByWebhookId(sessionCtx, req.WebhookId)
		if err != nil {
			return err
		}

		for _, event := range req.WebhookEvent {
			e := model.WebhookTriggerEvent(event)
			if !slices.Contains(model.AllWebhookTriggerEvent, e) {
				return fmt.Errorf("invalid webhook event, %s", e)
			}
			webhookEvent := &model.WebhookTrigger{
				WebhookId: webhook.ID,
				Event:     model.WebhookTriggerEvent(event),
			}
			err = webhookEventDao.Create(sessionCtx, webhookEvent)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return errs.Unwrap(err)
}

type DeleteWebhookReq struct {
	WebhookId primitive.ObjectID `json:"webhook_id" binding:"required"`
}

func (s *OrgWebhookSvc) DeleteWebhook(ctx context.Context, req DeleteWebhookReq, orgId primitive.ObjectID) error {
	db := plugin.MongoCli().GetDB()
	webhookDao := model.NewWebhookDao(db)
	webhookEventDao := model.NewWebhookTriggerDao(db)

	err := plugin.MongoCli().GetTx().Transaction(ctx, func(sessionCtx context.Context) error {
		webhook, err := webhookDao.GetById(sessionCtx, req.WebhookId)
		if err != nil {
			if dbutil.IsDBNotFound(err) {
				return freeErrors.NotFoundErrWithResource("webhook" + req.WebhookId.String())
			}
			return err
		}

		if webhook.OrganizationId != orgId {
			return fmt.Errorf("this webhook does not belong to this organization, webhook id: %s", webhook.ID.Hex())
		}

		err = webhookDao.DeleteById(sessionCtx, req.WebhookId)
		if err != nil {
			return err
		}

		err = webhookEventDao.DeleteByWebhookId(sessionCtx, req.WebhookId)
		if err != nil {
			return err
		}

		return nil
	})
	return errs.Unwrap(err)
}

func (s *OrgWebhookSvc) ListWebhook(ctx context.Context, orgId primitive.ObjectID, status *bool,
	keyword string, page *paginationUtils.DepPagination) (*paginationUtils.ListResp[*dto.WebhookResp], error) {
	db := plugin.MongoCli().GetDB()

	log.ZInfo(context.TODO(), "ListWebhook", "orgId", orgId.String(), "keyword", keyword)

	webhookDao := model.NewWebhookDao(db)

	total, items, err := webhookDao.Select(ctx, keyword, orgId, status, page)
	if err != nil {
		return nil, err
	}

	resp := &paginationUtils.ListResp[*dto.WebhookResp]{
		Total: total,
		List:  []*dto.WebhookResp{},
	}

	for _, item := range items {
		tsRecord, err := dto.NewWebhookResp(db, item)
		if err != nil {
			return nil, err
		}
		resp.List = append(resp.List, tsRecord)
	}

	return resp, nil
}

type SendWebhookBody struct {
	OrgId primitive.ObjectID        `json:"org_id"`
	Event model.WebhookTriggerEvent `json:"event"`
}

func (s *OrgWebhookSvc) TriggerWebhook(ctx context.Context, orgId primitive.ObjectID, event model.WebhookTriggerEvent, sendWebhookBody SendWebhookBody) error {
	db := plugin.MongoCli().GetDB()
	webhookDao := model.NewWebhookDao(db)

	status := true
	_, items, err := webhookDao.SelectJoinWebhookTrigger(ctx, orgId, &status, event)
	if err != nil {
		return err
	}

	for _, item := range items {
		go func(url string) {
			body, _ := json.Marshal(sendWebhookBody)

			client := &http.Client{}
			request, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
			if err != nil {
				log.ZError(ctx, "TriggerWebhook NewRequest error", err, "url", url)
				return
			}
			request.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(request) //发送请求
			if err != nil {
				log.ZError(ctx, "TriggerWebhook error", err, "url", url)
				return
			}
			defer resp.Body.Close()
		}(item.Url)
	}
	return nil
}
