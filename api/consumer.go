package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"bytes"
	"sequin-cli/context"
)

// ConsumersResponse represents the structure of the API response for a list of consumers
type ConsumersResponse struct {
	Consumers []Consumer `json:"data"`
}

type FetchNextMessagesResponse struct {
	Data []MessageWithAckToken `json:"data"`
}

// Consumer represents the structure of a consumer returned by the API
type Consumer struct {
	ID                   string    `json:"id"`
	Slug                 string    `json:"slug"`
	StreamID             string    `json:"stream_id"`
	AckWaitMS            int       `json:"ack_wait_ms"`
	MaxAckPending        int       `json:"max_ack_pending"`
	MaxDeliver           int       `json:"max_deliver"`
	MaxWaiting           int       `json:"max_waiting"`
	FilterSubjectPattern string    `json:"filter_subject_pattern"`
	CreatedAt            time.Time `json:"inserted_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// ConsumerCreateOptions represents the options for creating a new consumer
type ConsumerCreateOptions struct {
	Slug                 string `json:"slug"`
	StreamID             string `json:"stream_id"`
	AckWaitMS            int    `json:"ack_wait_ms,omitempty"`
	MaxAckPending        int    `json:"max_ack_pending,omitempty"`
	MaxDeliver           int    `json:"max_deliver,omitempty"`
	MaxWaiting           int    `json:"max_waiting,omitempty"`
	FilterSubjectPattern string `json:"filter_subject_pattern"`
}

// ConsumerUpdateOptions represents the options for updating an existing consumer
type ConsumerUpdateOptions struct {
	AckWaitMS     int `json:"ack_wait_ms,omitempty"`
	MaxAckPending int `json:"max_ack_pending,omitempty"`
	MaxDeliver    int `json:"max_deliver,omitempty"`
	MaxWaiting    int `json:"max_waiting,omitempty"`
}

// MessageInfo represents the structure of the info field in a MessageWithInfo
type MessageInfo struct {
	DeliverCount    int        `json:"deliver_count"`
	LastDeliveredAt *time.Time `json:"last_delivered_at"`
	NotVisibleUntil *time.Time `json:"not_visible_until"`
	State           string     `json:"state"`
}

// FormatLastDeliveredAt returns a formatted string for LastDeliveredAt
func (mi *MessageInfo) FormatLastDeliveredAt() string {
	if mi.LastDeliveredAt == nil {
		return "N/A"
	}
	return fmt.Sprintf("%s (%s ago)", mi.LastDeliveredAt.Format(time.RFC3339), time.Since(*mi.LastDeliveredAt).Round(time.Second))
}

// FormatNotVisibleUntil returns a formatted string for NotVisibleUntil
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

// MessageWithInfo represents the structure of a consumer message returned by the API
type MessageWithInfo struct {
	Message Message     `json:"message"`
	Info    MessageInfo `json:"info"`
}

type MessageWithAckToken struct {
	Message  Message `json:"message"`
	AckToken string  `json:"ack_token"`
}

// FetchConsumers retrieves all consumers for a stream from the API
func FetchConsumers(ctx *context.Context, streamID string) ([]Consumer, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(fmt.Sprintf("%s/api/streams/%s/consumers", serverURL, streamID))
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var consumersResponse ConsumersResponse
	err = json.Unmarshal(body, &consumersResponse)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return consumersResponse.Consumers, nil
}

// FetchConsumerInfo retrieves information for a specific consumer from the API
func FetchConsumerInfo(ctx *context.Context, streamID, consumerID string) (*Consumer, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(fmt.Sprintf("%s/api/streams/%s/consumers/%s", serverURL, streamID, consumerID))
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var consumer Consumer
	err = json.Unmarshal(body, &consumer)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &consumer, nil
}

// AddConsumer adds a new consumer to a stream
func AddConsumer(ctx *context.Context, options ConsumerCreateOptions) (*Consumer, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonBody, err := json.Marshal(options)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	// Create the POST request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/streams/%s/consumers", serverURL, options.StreamID), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	// Read the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	// Check for successful status code
	if resp.StatusCode != http.StatusOK {
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	// Unmarshal the response
	var consumer Consumer
	err = json.Unmarshal(body, &consumer)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &consumer, nil
}

// EditConsumer updates an existing consumer
func EditConsumer(ctx *context.Context, streamID, consumerID string, options ConsumerUpdateOptions) (*Consumer, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonBody, err := json.Marshal(options)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/api/streams/%s/consumers/%s", serverURL, streamID, consumerID), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var consumer Consumer
	err = json.Unmarshal(body, &consumer)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &consumer, nil
}

// FetchNextMessages retrieves the next batch of messages for a consumer
func FetchNextMessages(ctx *context.Context, streamID, consumerID string, batchSize int) ([]MessageWithAckToken, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(fmt.Sprintf("%s/api/streams/%s/consumers/%s/next?batch_size=%d", serverURL, streamID, consumerID, batchSize))
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var result FetchNextMessagesResponse
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return result.Data, nil
}

// FetchMessagesOptions represents the options for fetching messages
type FetchMessagesOptions struct {
	StreamID   string
	ConsumerID string
	Visible    bool
	Limit      int
	Order      string
}

// FetchMessages retrieves messages for a consumer with optional filters
func FetchMessages(ctx *context.Context, options FetchMessagesOptions) ([]MessageWithInfo, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/streams/%s/consumers/%s/messages?limit=%d&sort=%s",
		serverURL, options.StreamID, options.ConsumerID, options.Limit, options.Order)
	if !options.Visible {
		url += "&visible=false"
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var result struct {
		Data []MessageWithInfo `json:"data"`
	}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return result.Data, nil
}

// AckMessage acknowledges a message for a consumer
func AckMessage(ctx *context.Context, streamID, consumerID, ackID string) error {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return err
	}

	requestBody := map[string][]string{"ack_tokens": {ackID}}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/streams/%s/consumers/%s/ack", serverURL, streamID, consumerID), bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return ParseAPIError(resp.StatusCode, string(body))
	}

	return nil
}

// NackMessage negative acknowledges a message for a consumer
func NackMessage(ctx *context.Context, streamID, consumerID, ackID string) error {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return err
	}

	requestBody := map[string][]string{"ack_tokens": {ackID}}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/streams/%s/consumers/%s/nack", serverURL, streamID, consumerID), bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return ParseAPIError(resp.StatusCode, string(body))
	}

	return nil
}
