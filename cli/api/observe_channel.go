package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/nshafer/phx"
	"github.com/sequinstream/sequin/cli/context"
)

type ObserveChannel struct {
	socket  *phx.Socket
	channel *phx.Channel
}

func NewObserveChannel(ctx *context.Context) (*ObserveChannel, error) {
	endPoint, err := url.Parse(ctx.WebSocketURL)
	if err != nil {
		return nil, err
	}

	socket := phx.NewSocket(endPoint)
	socket.Logger = phx.NewSimpleLogger(phx.LogInfo)

	oc := &ObserveChannel{
		socket: socket,
	}

	return oc, nil
}

func (oc *ObserveChannel) Connect() error {
	err := oc.socket.Connect()
	if err != nil {
		return err
	}

	// Wait for the socket to connect
	cont := make(chan bool)
	oc.socket.OnOpen(func() {
		cont <- true
	})
	<-cont

	// Join the "observe" channel
	oc.channel = oc.socket.Channel("observe", nil)
	join, err := oc.channel.Join()
	if err != nil {
		return err
	}

	// Wait for join confirmation
	join.Receive("ok", func(response any) {
		log.Println("Joined channel:", oc.channel.Topic(), response)
		cont <- true
	})
	join.Receive("error", func(response any) {
		log.Println("Join error", response)
	})
	<-cont

	return nil
}

func (oc *ObserveChannel) OnStreamCreated(handler func(Stream)) {
	oc.channel.On("stream:created", func(payload any) {
		var stream Stream
		if err := parsePayload(payload, &stream); err != nil {
			log.Printf("Error parsing stream:created payload: %v", err)
			return
		}
		handler(stream)
	})
}

func (oc *ObserveChannel) OnStreamUpdated(handler func(Stream)) {
	oc.channel.On("stream:updated", func(payload any) {
		var stream Stream
		if err := parsePayload(payload, &stream); err != nil {
			log.Printf("Error parsing stream:updated payload: %v", err)
			return
		}
		handler(stream)
	})
}

func (oc *ObserveChannel) OnStreamDeleted(handler func(Stream)) {
	oc.channel.On("stream:deleted", func(payload any) {
		var stream Stream
		if err := parsePayload(payload, &stream); err != nil {
			log.Printf("Error parsing stream:deleted payload: %v", err)
			return
		}
		handler(stream)
	})
}

func (oc *ObserveChannel) OnConsumerCreated(handler func(Consumer)) {
	oc.channel.On("consumer:created", func(payload any) {
		var consumer Consumer
		if err := parsePayload(payload, &consumer); err != nil {
			log.Printf("Error parsing consumer:created payload: %v", err)
			return
		}
		handler(consumer)
	})
}

func (oc *ObserveChannel) OnConsumerUpdated(handler func(Consumer)) {
	oc.channel.On("consumer:updated", func(payload any) {
		var consumer Consumer
		if err := parsePayload(payload, &consumer); err != nil {
			log.Printf("Error parsing consumer:updated payload: %v", err)
			return
		}
		handler(consumer)
	})
}

func (oc *ObserveChannel) OnConsumerDeleted(handler func(Consumer)) {
	oc.channel.On("consumer:deleted", func(payload any) {
		var consumer Consumer
		if err := parsePayload(payload, &consumer); err != nil {
			log.Printf("Error parsing consumer:deleted payload: %v", err)
			return
		}
		handler(consumer)
	})
}

func (oc *ObserveChannel) OnMessagesUpserted(handler func([]Message)) {
	oc.channel.On("messages:upserted", func(payload any) {
		type tempPayload struct {
			Messages []Message `json:"messages"`
		}

		var tp tempPayload
		if err := parsePayload(payload, &tp); err != nil {
			log.Printf("Error parsing messages:upserted payload: %v", err)
			return
		}

		handler(tp.Messages)
	})
}

func parsePayload(payload any, target interface{}) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %w", err)
	}
	if err := json.Unmarshal(jsonData, target); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}
	return nil
}
