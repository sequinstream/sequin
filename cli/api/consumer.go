package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"bytes"

	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type ConsumersResponse struct {
	Consumers []models.Consumer `json:"data"`
}

type ConsumerCreateOptions struct {
	Name             string                       `json:"name"`
	StreamID         string                       `json:"stream_id"`
	AckWaitMS        int                          `json:"ack_wait_ms,omitempty"`
	MaxAckPending    int                          `json:"max_ack_pending,omitempty"`
	MaxDeliver       int                          `json:"max_deliver,omitempty"`
	FilterKeyPattern string                       `json:"filter_key_pattern"`
	Kind             string                       `json:"kind"`
	HttpEndpoint     *ConsumerHttpEndpointOptions `json:"http_endpoint,omitempty"`
}

type ConsumerUpdateOptions struct {
	AckWaitMS     int                          `json:"ack_wait_ms,omitempty"`
	MaxAckPending int                          `json:"max_ack_pending,omitempty"`
	MaxDeliver    int                          `json:"max_deliver,omitempty"`
	Kind          *string                      `json:"kind,omitempty"`
	HttpEndpoint  *ConsumerHttpEndpointOptions `json:"http_endpoint,omitempty"`
}
type ConsumerHttpEndpointOptions struct {
	BaseURL string            `json:"base_url"`
	Headers map[string]string `json:"headers"`
}

// BuildFetchConsumers builds the HTTP request for fetching consumers
func BuildFetchConsumers(ctx *context.Context, streamID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/streams/%s/consumers", serverURL, streamID), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// FetchConsumers retrieves all consumers for a stream from the API
func FetchConsumers(ctx *context.Context, streamID string) ([]models.Consumer, error) {
	req, err := BuildFetchConsumers(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("error building fetch consumers request: %w", err)
	}

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

	var consumersResponse ConsumersResponse
	err = json.Unmarshal(body, &consumersResponse)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return consumersResponse.Consumers, nil
}

// BuildFetchConsumerInfo builds the HTTP request for fetching consumer info
func BuildFetchConsumerInfo(ctx *context.Context, streamID, consumerID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/streams/%s/consumers/%s", serverURL, streamID, consumerID), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// FetchConsumerInfo retrieves information for a specific consumer from the API
func FetchConsumerInfo(ctx *context.Context, streamID, consumerID string) (*models.Consumer, error) {
	req, err := BuildFetchConsumerInfo(ctx, streamID, consumerID)
	if err != nil {
		return nil, fmt.Errorf("error building fetch consumer info request: %w", err)
	}

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

	var consumer models.Consumer
	err = json.Unmarshal(body, &consumer)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &consumer, nil
}

// BuildAddConsumer builds the HTTP request for adding a new consumer
func BuildAddConsumer(ctx *context.Context, options ConsumerCreateOptions) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonBody, err := json.Marshal(options)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/streams/%s/consumers", serverURL, options.StreamID), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

// AddConsumer adds a new consumer to a stream
func AddConsumer(ctx *context.Context, options ConsumerCreateOptions) (*models.Consumer, error) {
	req, err := BuildAddConsumer(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("error building add consumer request: %w", err)
	}

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

	var consumer models.Consumer
	err = json.Unmarshal(body, &consumer)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &consumer, nil
}

// BuildEditConsumer builds the HTTP request for editing an existing consumer
func BuildEditConsumer(ctx *context.Context, streamID, consumerID string, options ConsumerUpdateOptions) (*http.Request, error) {
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

	return req, nil
}

// EditConsumer updates an existing consumer
func EditConsumer(ctx *context.Context, streamID, consumerID string, options ConsumerUpdateOptions) (*models.Consumer, error) {
	req, err := BuildEditConsumer(ctx, streamID, consumerID, options)
	if err != nil {
		return nil, fmt.Errorf("error building edit consumer request: %w", err)
	}

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

	var consumer models.Consumer
	err = json.Unmarshal(body, &consumer)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &consumer, nil
}

// BuildReceiveMessages builds the HTTP request for fetching next messages
func BuildReceiveMessages(ctx *context.Context, streamID, consumerID string, batchSize int) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/streams/%s/consumers/%s/receive?batch_size=%d", serverURL, streamID, consumerID, batchSize), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

type ReceiveMessagesResponse struct {
	Data []models.MessageWithAckID `json:"data"`
}

// ReceiveMessages retrieves the next batch of messages for a consumer
func ReceiveMessages(ctx *context.Context, streamID, consumerID string, batchSize int) ([]models.MessageWithAckID, error) {
	req, err := BuildReceiveMessages(ctx, streamID, consumerID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("error building fetch receive messages request: %w", err)
	}

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

	var result ReceiveMessagesResponse
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return result.Data, nil
}

type FetchMessagesOptions struct {
	StreamIDOrName string
	ConsumerID     string
	Visible        bool
	Limit          int
	Order          string
}

// BuildFetchMessages builds the HTTP request for fetching messages
func BuildFetchMessages(ctx *context.Context, options FetchMessagesOptions) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/streams/%s/consumers/%s/messages?limit=%d&sort=%s",
		serverURL, options.StreamIDOrName, options.ConsumerID, options.Limit, options.Order)
	if !options.Visible {
		url += "&visible=false"
	} else if options.Visible {
		url += "&visible=true"
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// FetchMessages retrieves messages for a consumer with optional filters
func FetchMessages(ctx *context.Context, options FetchMessagesOptions) ([]models.MessageWithInfo, error) {
	req, err := BuildFetchMessages(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("error building fetch messages request: %w", err)
	}

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

	var result struct {
		Data []models.MessageWithInfo `json:"data"`
	}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return result.Data, nil
}

// BuildAckMessage builds the HTTP request for acknowledging a message
func BuildAckMessage(ctx *context.Context, streamID, consumerID, ackID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	requestBody := map[string][]string{"ack_ids": {ackID}}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/streams/%s/consumers/%s/ack", serverURL, streamID, consumerID), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

// AckMessage acknowledges a message for a consumer
func AckMessage(ctx *context.Context, streamID, consumerID, ackID string) error {
	req, err := BuildAckMessage(ctx, streamID, consumerID, ackID)
	if err != nil {
		return fmt.Errorf("error building ack message request: %w", err)
	}

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

// BuildNackMessage builds the HTTP request for negative acknowledging a message
func BuildNackMessage(ctx *context.Context, streamID, consumerID, ackID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	requestBody := map[string][]string{"ack_ids": {ackID}}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/streams/%s/consumers/%s/nack", serverURL, streamID, consumerID), bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

// NackMessage negative acknowledges a message for a consumer
func NackMessage(ctx *context.Context, streamID, consumerID, ackID string) error {
	req, err := BuildNackMessage(ctx, streamID, consumerID, ackID)
	if err != nil {
		return fmt.Errorf("error building nack message request: %w", err)
	}

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

// BuildRemoveConsumer builds the HTTP request for removing a consumer
func BuildRemoveConsumer(ctx *context.Context, streamID, consumerID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/streams/%s/consumers/%s", serverURL, streamID, consumerID), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// RemoveConsumer removes a consumer from a stream
func RemoveConsumer(ctx *context.Context, streamID, consumerID string) error {
	req, err := BuildRemoveConsumer(ctx, streamID, consumerID)
	if err != nil {
		return fmt.Errorf("error building remove consumer request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return ParseAPIError(resp.StatusCode, string(body))
	}

	return nil
}
