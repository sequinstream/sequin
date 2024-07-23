package api

import (
	"log"

	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type WebhookChannel struct {
	BaseChannel
}

func NewWebhookChannel(ctx *context.Context) (*WebhookChannel, error) {
	baseChannel, err := NewBaseChannel(ctx, "webhook", &SilentLogger{})
	if err != nil {
		return nil, err
	}

	wc := &WebhookChannel{
		BaseChannel: *baseChannel,
	}

	return wc, nil
}

func (wc *WebhookChannel) Connect() error {
	return wc.BaseChannel.Connect("webhook")
}

func (wc *WebhookChannel) OnWebhookIngested(handler func(models.Webhook, models.Message)) {
	wc.channel.On("webhook:ingested", func(payload any) {
		var webhookPayload struct {
			Webhook models.Webhook `json:"webhook"`
			Message models.Message `json:"message"`
		}
		if err := parsePayload(payload, &webhookPayload); err != nil {
			log.Printf("Error parsing webhook:ingested payload: %v", err)
			return
		}
		handler(webhookPayload.Webhook, webhookPayload.Message)
	})
}
