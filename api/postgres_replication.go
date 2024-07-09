package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"sequin-cli/context"
)

// PostgresReplicationsResponse represents the structure of the API response for a list of PostgresReplications
type PostgresReplicationsResponse struct {
	PostgresReplications []PostgresReplication `json:"data"`
}

// PostgresReplication represents the structure of a PostgresReplication returned by the API
type PostgresReplication struct {
	ID                 string    `json:"id"`
	SlotName           string    `json:"slot_name"`
	PublicationName    string    `json:"publication_name"`
	Status             string    `json:"status"`
	AccountID          string    `json:"account_id"`
	PostgresDatabaseID string    `json:"postgres_database_id"`
	StreamID           string    `json:"stream_id"`
	CreatedAt          time.Time `json:"inserted_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// PostgresReplicationCreate represents the structure for creating a new PostgresReplication
type PostgresReplicationCreate struct {
	SlotName         string `json:"slot_name"`
	PublicationName  string `json:"publication_name"`
	Status           string `json:"status,omitempty"`
	StreamID         string `json:"stream_id"`
	PostgresDatabase struct {
		Database string `json:"database"`
		Hostname string `json:"hostname"`
		Port     int    `json:"port"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"postgres_database"`
}

// FetchPostgresReplications retrieves all PostgresReplications from the API
func FetchPostgresReplications(ctx *context.Context) ([]PostgresReplication, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(serverURL + "/api/postgres_replications")
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	var pgReplicationsResponse PostgresReplicationsResponse
	err = json.Unmarshal(body, &pgReplicationsResponse)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return pgReplicationsResponse.PostgresReplications, nil
}

// FetchPostgresReplicationInfo retrieves information for a specific PostgresReplication from the API
func FetchPostgresReplicationInfo(ctx *context.Context, replicationID string) (*PostgresReplication, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(fmt.Sprintf("%s/api/postgres_replications/%s", serverURL, replicationID))
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	var pgReplication PostgresReplication
	err = json.Unmarshal(body, &pgReplication)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &pgReplication, nil
}

// CreatePostgresReplication creates a new PostgresReplication
func CreatePostgresReplication(ctx *context.Context, replicationData *PostgresReplicationCreate) (*PostgresReplication, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(replicationData)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	resp, err := http.Post(serverURL+"/api/postgres_replications", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	var pgReplication PostgresReplication
	err = json.Unmarshal(body, &pgReplication)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &pgReplication, nil
}

// DeletePostgresReplication deletes a PostgresReplication
func DeletePostgresReplication(ctx *context.Context, replicationID string) error {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/postgres_replications/%s", serverURL, replicationID), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
