package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type WebhooksResponse struct {
	Webhooks []models.Webhook `json:"data"`
}

type WebhookCreateOptions struct {
	Name     string `json:"name"`
	StreamID string `json:"stream_id"`
}

// BuildFetchWebhooks builds the HTTP request for fetching all webhooks
func BuildFetchWebhooks(ctx *context.Context) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", serverURL+"/api/webhooks", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// FetchWebhooks retrieves all Webhooks from the API
func FetchWebhooks(ctx *context.Context) ([]models.Webhook, error) {
	req, err := BuildFetchWebhooks(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
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

	var webhooksResponse WebhooksResponse
	err = json.Unmarshal(body, &webhooksResponse)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return webhooksResponse.Webhooks, nil
}

// BuildFetchWebhook builds the HTTP request for fetching a specific webhook
func BuildFetchWebhook(ctx *context.Context, idOrName string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/webhooks/%s", serverURL, idOrName), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// FetchWebhook retrieves a specific Webhook from the API
func FetchWebhook(ctx *context.Context, idOrName string) (*models.Webhook, error) {
	req, err := BuildFetchWebhook(ctx, idOrName)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
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

	var webhook models.Webhook
	err = json.Unmarshal(body, &webhook)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &webhook, nil
}

// BuildCreateWebhook builds the HTTP request for creating a new Webhook
func BuildCreateWebhook(ctx *context.Context, webhookData *WebhookCreateOptions) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(webhookData)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", serverURL+"/api/webhooks", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

// CreateWebhook creates a new Webhook
func CreateWebhook(ctx *context.Context, webhookData *WebhookCreateOptions) (*models.Webhook, error) {
	req, err := BuildCreateWebhook(ctx, webhookData)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		err := ParseAPIError(resp.StatusCode, string(body))
		switch e := err.(type) {
		case *ValidationError:
			return nil, fmt.Errorf(e.Error())
		case *APIError:
			return nil, fmt.Errorf("error creating webhook: %s", e.Body)
		default:
			return nil, fmt.Errorf("error creating webhook: %s", string(body))
		}
	}

	var webhook models.Webhook
	err = json.Unmarshal(body, &webhook)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &webhook, nil
}

// BuildUpdateWebhook builds the HTTP request for updating an existing Webhook
func BuildUpdateWebhook(ctx *context.Context, idOrName string, webhookData *WebhookCreateOptions) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(webhookData)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/api/webhooks/%s", serverURL, idOrName), bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

// UpdateWebhook updates an existing Webhook
func UpdateWebhook(ctx *context.Context, idOrName string, webhookData *WebhookCreateOptions) (*models.Webhook, error) {
	req, err := BuildUpdateWebhook(ctx, idOrName, webhookData)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
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

	var webhook models.Webhook
	err = json.Unmarshal(body, &webhook)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &webhook, nil
}

// BuildDeleteWebhook builds the HTTP request for deleting a Webhook
func BuildDeleteWebhook(ctx *context.Context, idOrName string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/webhooks/%s", serverURL, idOrName), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// DeleteWebhook deletes a Webhook
func DeleteWebhook(ctx *context.Context, idOrName string) error {
	req, err := BuildDeleteWebhook(ctx, idOrName)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
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
