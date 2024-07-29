package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/nshafer/phx"
	"github.com/sequinstream/sequin/cli/context"
)

type BaseChannel struct {
	socket  *phx.Socket
	channel *phx.Channel
}

func NewBaseChannel(ctx *context.Context, channelName string, logger phx.Logger) (*BaseChannel, error) {
	webSocketURL, err := context.GetWebSocketURL(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get WebSocket URL: %w", err)
	}

	endPoint, err := url.Parse(webSocketURL)
	if err != nil {
		return nil, err
	}

	socket := phx.NewSocket(endPoint)
	socket.Logger = logger

	bc := &BaseChannel{
		socket: socket,
	}

	return bc, nil
}

func (bc *BaseChannel) Connect(channelName string) error {
	err := bc.socket.Connect()
	if err != nil {
		return err
	}

	// Wait for the socket to connect
	cont := make(chan bool)
	bc.socket.OnOpen(func() {
		cont <- true
	})
	<-cont

	// Join the channel
	bc.channel = bc.socket.Channel(channelName, nil)
	join, err := bc.channel.Join()
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

type SilentLogger struct{}

func (l *SilentLogger) Print(_ phx.LoggerLevel, _ string, _ ...any)            {}
func (l *SilentLogger) Println(_ phx.LoggerLevel, _ string, _ ...any)          {}
func (l *SilentLogger) Printf(_ phx.LoggerLevel, _ string, _ string, _ ...any) {}
