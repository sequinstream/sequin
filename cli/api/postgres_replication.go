package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sequinstream/sequin/cli/context"
)

// PostgresReplicationsResponse represents the structure of the API response for a list of PostgresReplications
type PostgresReplicationsResponse struct {
	PostgresReplications []PostgresReplication `json:"data"`
}

// PostgresReplication represents the structure of a PostgresReplication returned by the API
type PostgresReplication struct {
	ID                  string     `json:"id"`
	SlotName            string     `json:"slot_name"`
	PublicationName     string     `json:"publication_name"`
	Status              string     `json:"status"`
	AccountID           string     `json:"account_id"`
	PostgresDatabaseID  string     `json:"postgres_database_id"`
	SSL                 bool       `json:"ssl"`
	StreamID            string     `json:"stream_id"`
	CreatedAt           time.Time  `json:"inserted_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	BackfillCompletedAt *time.Time `json:"backfill_completed_at"`
	KeyFormat           string     `json:"key_format"`
}

// Add this helper function to format BackfillCompletedAt
func (pr *PostgresReplication) FormatBackfillCompletedAt() string {
	if pr.BackfillCompletedAt == nil {
		if pr.Status == "backfilling" {
			return "Not completed"
		}
		return "N/A"
	}
	return fmt.Sprintf("%s (%s ago)", pr.BackfillCompletedAt.Format(time.RFC3339), time.Since(*pr.BackfillCompletedAt).Round(time.Second))
}

// PostgresReplicationCreate represents the structure for creating a new PostgresReplication
type PostgresReplicationCreate struct {
	SlotName             string `json:"slot_name"`
	PublicationName      string `json:"publication_name"`
	SSL                  bool   `json:"ssl"`
	StreamID             string `json:"stream_id"`
	PostgresDatabaseID   string `json:"postgres_database_id"`
	BackfillExistingRows bool   `json:"backfill_existing_rows"`
	KeyFormat            string `json:"key_format"`
}

// PostgresReplicationInfo represents the additional info for a PostgresReplication
type PostgresReplicationInfo struct {
	LastCommittedAt       *time.Time `json:"last_committed_at"`
	TotalIngestedMessages int64      `json:"total_ingested_messages"`
}

// PostgresReplicationWithInfo represents the structure of a PostgresReplication with additional info
type PostgresReplicationWithInfo struct {
	PostgresReplication PostgresReplication     `json:"postgres_replication"`
	Info                PostgresReplicationInfo `json:"info"`
}

// FormatLastCommittedAt returns a formatted string for LastCommittedAt
func (pri *PostgresReplicationInfo) FormatLastCommittedAt() string {
	if pri.LastCommittedAt == nil {
		return "N/A"
	}
	return fmt.Sprintf("%s (%s ago)", pri.LastCommittedAt.Format(time.RFC3339), time.Since(*pri.LastCommittedAt).Round(time.Second))
}

// BuildFetchPostgresReplications builds the HTTP request for fetching postgres replications
func BuildFetchPostgresReplications(ctx *context.Context) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", serverURL+"/api/postgres_replications", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// FetchPostgresReplications retrieves all PostgresReplications from the API
func FetchPostgresReplications(ctx *context.Context) ([]PostgresReplication, error) {
	req, err := BuildFetchPostgresReplications(ctx)
	if err != nil {
		return nil, fmt.Errorf("error building fetch postgres replications request: %w", err)
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

	var pgReplicationsResponse PostgresReplicationsResponse
	err = json.Unmarshal(body, &pgReplicationsResponse)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return pgReplicationsResponse.PostgresReplications, nil
}

// BuildFetchPostgresReplicationInfo builds the HTTP request for fetching postgres replication info
func BuildFetchPostgresReplicationInfo(ctx *context.Context, replicationID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/postgres_replications/%s", serverURL, replicationID), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// FetchPostgresReplicationInfo retrieves information for a specific PostgresReplication from the API
func FetchPostgresReplicationInfo(ctx *context.Context, replicationID string) (*PostgresReplicationWithInfo, error) {
	req, err := BuildFetchPostgresReplicationInfo(ctx, replicationID)
	if err != nil {
		return nil, fmt.Errorf("error building fetch postgres replication info request: %w", err)
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

	var pgReplicationWithInfo PostgresReplicationWithInfo
	err = json.Unmarshal(body, &pgReplicationWithInfo)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &pgReplicationWithInfo, nil
}

// BuildAddPostgresReplication builds the HTTP request for adding a new postgres replication
func BuildAddPostgresReplication(ctx *context.Context, replicationData *PostgresReplicationCreate) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(replicationData)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", serverURL+"/api/postgres_replications", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

// AddPostgresReplication creates a new PostgresReplication
func AddPostgresReplication(ctx *context.Context, replicationData *PostgresReplicationCreate) (*PostgresReplication, error) {
	req, err := BuildAddPostgresReplication(ctx, replicationData)
	if err != nil {
		return nil, fmt.Errorf("error building add postgres replication request: %w", err)
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

	var pgReplication PostgresReplication
	err = json.Unmarshal(body, &pgReplication)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &pgReplication, nil
}

// BuildDeletePostgresReplication builds the HTTP request for deleting a postgres replication
func BuildDeletePostgresReplication(ctx *context.Context, replicationID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/postgres_replications/%s", serverURL, replicationID), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

// DeletePostgresReplication deletes a PostgresReplication
func DeletePostgresReplication(ctx *context.Context, replicationID string) error {
	req, err := BuildDeletePostgresReplication(ctx, replicationID)
	if err != nil {
		return fmt.Errorf("error building delete postgres replication request: %w", err)
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

// BuildUpdatePostgresReplication builds the HTTP request for updating a postgres replication
func BuildUpdatePostgresReplication(ctx *context.Context, replicationID string, updateOptions PostgresReplicationUpdate) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(updateOptions)
	if err != nil {
		return nil, fmt.Errorf("error marshaling update options: %w", err)
	}

	req, err := http.NewRequest("PATCH", fmt.Sprintf("%s/api/postgres_replications/%s", serverURL, replicationID), bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

// UpdatePostgresReplication updates an existing PostgresReplication
func UpdatePostgresReplication(ctx *context.Context, replicationID string, updateOptions PostgresReplicationUpdate) (*PostgresReplication, error) {
	req, err := BuildUpdatePostgresReplication(ctx, replicationID, updateOptions)
	if err != nil {
		return nil, fmt.Errorf("error building update postgres replication request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var pgReplication PostgresReplication
	if err := json.NewDecoder(resp.Body).Decode(&pgReplication); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &pgReplication, nil
}

// PostgresReplicationUpdate represents the structure for updating a PostgresReplication
type PostgresReplicationUpdate struct {
	BackfillExistingRows bool   `json:"backfill_existing_rows,omitempty"`
	KeyFormat            string `json:"key_format,omitempty"`
	SSL                  bool   `json:"ssl,omitempty"`
}
