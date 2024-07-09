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

// StreamsResponse represents the structure of the API response for a list
type StreamsResponse struct {
	Streams []Stream `json:"data"`
}

// Stream represents the structure of a stream returned by the API
type Stream struct {
	ID            string    `json:"id"`
	Idx           int       `json:"idx"`
	Slug          string    `json:"slug"`
	ConsumerCount int       `json:"consumer_count"`
	MessageCount  int       `json:"message_count"`
	CreatedAt     time.Time `json:"inserted_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// FetchStreams retrieves all streams from the API
func FetchStreams(ctx *context.Context) ([]Stream, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(serverURL + "/api/streams")
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	var streamsResponse StreamsResponse
	err = json.Unmarshal(body, &streamsResponse)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return streamsResponse.Streams, nil
}

// FetchStreamInfo retrieves information for a specific stream from the API
func FetchStreamInfo(ctx *context.Context, streamID string) (*Stream, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(fmt.Sprintf("%s/api/streams/%s", serverURL, streamID))
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	var streamResponse Stream
	err = json.Unmarshal(body, &streamResponse)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &streamResponse, nil
}

// AddStream adds a new stream with the given slug
func AddStream(ctx *context.Context, slug string) (*Stream, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	// Create the request body
	requestBody := map[string]string{"slug": slug}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	// Create the POST request
	req, err := http.NewRequest("POST", serverURL+"/api/streams", bytes.NewBuffer(jsonBody))
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
		if resp.StatusCode == http.StatusUnprocessableEntity {
			var errorResponse struct {
				Summary          string              `json:"summary"`
				ValidationErrors map[string][]string `json:"validation_errors"`
			}
			if err := json.Unmarshal(body, &errorResponse); err == nil {
				for field, errors := range errorResponse.ValidationErrors {
					for _, errMsg := range errors {
						fmt.Printf("`%s` %s\n", field, errMsg)
					}
				}
				return nil, fmt.Errorf("validation failed")
			}
		}
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Unmarshal the response
	var stream Stream
	err = json.Unmarshal(body, &stream)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &stream, nil
}

// RemoveStream removes a stream with the given ID
func RemoveStream(ctx *context.Context, streamID string) error {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return err
	}

	// Create the DELETE request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/streams/%s", serverURL, streamID), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	// Check for successful status code
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}
