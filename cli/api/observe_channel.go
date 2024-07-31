package api

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type ObserveChannel struct {
	BaseChannel
	mu                       sync.RWMutex
	consumerMessagesCallback func(pendingMessages, upcomingMessages []models.ConsumerMessage)
}

func NewObserveChannel(ctx *context.Context) (*ObserveChannel, error) {
	baseChannel, err := NewBaseChannel(ctx, "observe", &SilentLogger{})
	if err != nil {
		return nil, fmt.Errorf("failed to create base channel: %w", err)
	}

	oc := &ObserveChannel{
		BaseChannel: *baseChannel,
	}

	// Ensure the channel is connected before setting up event handlers
	err = oc.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to channel: %w", err)
	}

	oc.channel.On("consumer_messages", func(payload interface{}) {
		var messages models.ConsumerMessages

		jsonData, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error marshaling payload: %v", err)
			return
		}

		if err := json.Unmarshal(jsonData, &messages); err != nil {
			log.Printf("Error parsing consumer_messages payload: %v", err)
			return
		}

		oc.consumerMessagesCallback(messages.PendingMessages, messages.UpcomingMessages)
	})

	return oc, nil
}

func (oc *ObserveChannel) ListenConsumer(consumerID string, limit int) error {
	_, err := oc.channel.Push("listen_consumer", map[string]interface{}{
		"consumer_id": consumerID,
		"limit":       limit,
	})
	return err
}

func (oc *ObserveChannel) ClearListeningConsumer() error {
	_, err := oc.channel.Push("clear_listening_consumer", nil)
	return err
}

func (oc *ObserveChannel) SetConsumerMessagesCallback(callback func(pendingMessages, upcomingMessages []models.ConsumerMessage)) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.consumerMessagesCallback = callback
}

func (oc *ObserveChannel) Connect() error {
	return oc.BaseChannel.Connect("observe")
}

func (oc *ObserveChannel) OnStreamCreated(handler func(models.Stream)) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.channel.On("stream:created", func(payload any) {
		var stream models.Stream
		if err := parsePayload(payload, &stream); err != nil {
			log.Printf("Error parsing stream:created payload: %v", err)
			return
		}
		handler(stream)
	})
}

func (oc *ObserveChannel) OnStreamUpdated(handler func(models.Stream)) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.channel.On("stream:updated", func(payload any) {
		var stream models.Stream
		if err := parsePayload(payload, &stream); err != nil {
			log.Printf("Error parsing stream:updated payload: %v", err)
			return
		}
		handler(stream)
	})
}

func (oc *ObserveChannel) OnStreamDeleted(handler func(models.Stream)) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.channel.On("stream:deleted", func(payload any) {
		var stream models.Stream
		if err := parsePayload(payload, &stream); err != nil {
			log.Printf("Error parsing stream:deleted payload: %v", err)
			return
		}
		handler(stream)
	})
}

func (oc *ObserveChannel) OnConsumerCreated(handler func(models.Consumer)) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.channel.On("consumer:created", func(payload any) {
		var consumer models.Consumer
		if err := parsePayload(payload, &consumer); err != nil {
			log.Printf("Error parsing consumer:created payload: %v", err)
			return
		}
		handler(consumer)
	})
}

func (oc *ObserveChannel) OnConsumerUpdated(handler func(models.Consumer)) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.channel.On("consumer:updated", func(payload any) {
		var consumer models.Consumer
		if err := parsePayload(payload, &consumer); err != nil {
			log.Printf("Error parsing consumer:updated payload: %v", err)
			return
		}
		handler(consumer)
	})
}

func (oc *ObserveChannel) OnConsumerDeleted(handler func(models.Consumer)) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.channel.On("consumer:deleted", func(payload any) {
		var consumer models.Consumer
		if err := parsePayload(payload, &consumer); err != nil {
			log.Printf("Error parsing consumer:deleted payload: %v", err)
			return
		}
		handler(consumer)
	})
}

func (oc *ObserveChannel) OnMessagesUpserted(handler func([]models.Message)) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.channel.On("messages:upserted", func(payload any) {
		type tempPayload struct {
			Messages []models.Message `json:"messages"`
		}

		var tp tempPayload
		if err := parsePayload(payload, &tp); err != nil {
			log.Printf("Error parsing messages:upserted payload: %v", err)
			return
		}

		handler(tp.Messages)
	})
}
