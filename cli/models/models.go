package models

import (
	"fmt"
	"time"
)

// Stream-related structures
type Stream struct {
	ID        string      `json:"id"`
	Idx       int         `json:"idx"`
	Name      string      `json:"name"`
	Stats     StreamStats `json:"stats"`
	CreatedAt time.Time   `json:"inserted_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

type StreamStats struct {
	ConsumerCount int `json:"consumer_count"`
	MessageCount  int `json:"message_count"`
	StorageSize   int `json:"storage_size"`
}

// Message-related structures
type Message struct {
	CreatedAt time.Time `json:"inserted_at"`
	Data      string    `json:"data"`
	Seq       int       `json:"seq"`
	Key       string    `json:"key"`
	UpdatedAt time.Time `json:"updated_at"`
	StreamID  string    `json:"stream_id"`
}

type MessageInfo struct {
	DeliverCount    int        `json:"deliver_count"`
	LastDeliveredAt *time.Time `json:"last_delivered_at"`
	NotVisibleUntil *time.Time `json:"not_visible_until"`
	State           string     `json:"state"`
	Key             string     `json:"key"`
	StreamID        string     `json:"stream_id"`
	DataHash        string     `json:"data_hash"`
	Data            string     `json:"data"`
	Seq             int        `json:"seq"`
	InsertedAt      time.Time  `json:"inserted_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type MessageWithInfo struct {
	Message Message     `json:"message"`
	Info    MessageInfo `json:"info"`
}

type MessageWithAckID struct {
	Message Message `json:"message"`
	AckID   string  `json:"ack_id"`
}

type MessageWithConsumerInfos struct {
	Message       Message        `json:"message"`
	ConsumerInfos []ConsumerInfo `json:"consumer_info"`
}

// Consumer-related structures
type Consumer struct {
	ID               string               `json:"id"`
	Name             string               `json:"name"`
	StreamID         string               `json:"stream_id"`
	AckWaitMS        int                  `json:"ack_wait_ms"`
	MaxAckPending    int                  `json:"max_ack_pending"`
	MaxDeliver       int                  `json:"max_deliver"`
	FilterKeyPattern string               `json:"filter_key_pattern"`
	CreatedAt        time.Time            `json:"inserted_at"`
	UpdatedAt        time.Time            `json:"updated_at"`
	Kind             string               `json:"kind"`
	HttpEndpoint     ConsumerHttpEndpoint `json:"http_endpoint,omitempty"`
}

type ConsumerInfo struct {
	State                    string     `json:"state"`
	NotVisibleUntil          *time.Time `json:"not_visible_until"`
	ConsumerID               string     `json:"consumer_id"`
	AckID                    *string    `json:"ack_id"`
	DeliverCount             *int       `json:"deliver_count"`
	LastDeliveredAt          *time.Time `json:"last_delivered_at"`
	ConsumerFilterKeyPattern string     `json:"consumer_filter_key_pattern"`
	ConsumerName             string     `json:"consumer_name"`
}

// HTTP-related structures
type ConsumerHttpEndpoint struct {
	BaseURL string            `json:"base_url"`
	Headers map[string]string `json:"headers"`
}

// Webhook-related structures
type Webhook struct {
	ID           string               `json:"id"`
	Name         string               `json:"name"`
	AccountID    string               `json:"account_id"`
	StreamID     string               `json:"stream_id"`
	Stream       Stream               `json:"stream"`
	CreatedAt    time.Time            `json:"inserted_at"`
	UpdatedAt    time.Time            `json:"updated_at"`
	AuthStrategy *WebhookAuthStrategy `json:"auth_strategy,omitempty"`
}

type WebhookAuthStrategy struct {
	Type       string `json:"type"`
	HeaderName string `json:"header_name,omitempty"`
	Secret     string `json:"secret,omitempty"`
}

// Helper methods
func (mi *MessageInfo) FormatLastDeliveredAt() string {
	if mi.LastDeliveredAt == nil {
		return "N/A"
	}
	return fmt.Sprintf("%s (%s ago)", mi.LastDeliveredAt.Format(time.RFC3339), time.Since(*mi.LastDeliveredAt).Round(time.Second))
}

func (mi *MessageInfo) FormatNotVisibleUntil() string {
	if mi.NotVisibleUntil == nil {
		return "N/A"
	}
	notVisibleUntil := *mi.NotVisibleUntil
	if notVisibleUntil.After(time.Now()) {
		return fmt.Sprintf("%s (%s from now)", notVisibleUntil.Format(time.RFC3339), time.Until(notVisibleUntil).Round(time.Second))
	}
	return fmt.Sprintf("%s (%s ago)", notVisibleUntil.Format(time.RFC3339), time.Since(notVisibleUntil).Round(time.Second))
}
