package api

import (
	"log"
	"net/url"

	"github.com/nshafer/phx"
	"github.com/sequinstream/sequin/cli/context"
)

type WebhookChannel struct {
	socket  *phx.Socket
	channel *phx.Channel
}

func NewWebhookChannel(ctx *context.Context) (*WebhookChannel, error) {
	endPoint, err := url.Parse(ctx.WebSocketURL)
	if err != nil {
		return nil, err
	}

	socket := phx.NewSocket(endPoint)
	socket.Logger = &silentLogger{}

	wc := &WebhookChannel{
		socket: socket,
	}

	return wc, nil
}

func (wc *WebhookChannel) Connect() error {
	err := wc.socket.Connect()
	if err != nil {
		return err
	}

	// Wait for the socket to connect
	cont := make(chan bool)
	wc.socket.OnOpen(func() {
		cont <- true
	})
	<-cont

	// Join the "webhook" channel
	wc.channel = wc.socket.Channel("webhook", nil)
	join, err := wc.channel.Join()
	if err != nil {
		return err
	}

	// Wait for join confirmation
	join.Receive("ok", func(response any) {
		cont <- true
	})
	join.Receive("error", func(response any) {
		log.Println("Join error", response)
	})
	<-cont

	return nil
}

func (wc *WebhookChannel) OnWebhookIngested(handler func(Webhook, Message)) {
	wc.channel.On("webhook:ingested", func(payload any) {
		var webhookPayload struct {
			Webhook Webhook `json:"webhook"`
			Message Message `json:"message"`
		}
		if err := parsePayload(payload, &webhookPayload); err != nil {
			log.Printf("Error parsing webhook:ingested payload: %v", err)
			return
		}
		handler(webhookPayload.Webhook, webhookPayload.Message)
	})
}

// Reuse the parsePayload function from observe_channel.go
// func parsePayload(payload any, target interface{}) error { ... }

type silentLogger struct{}

func (l *silentLogger) Print(_ phx.LoggerLevel, _ string, _ ...any)            {}
func (l *silentLogger) Println(_ phx.LoggerLevel, _ string, _ ...any)          {}
func (l *silentLogger) Printf(_ phx.LoggerLevel, _ string, _ string, _ ...any) {}
