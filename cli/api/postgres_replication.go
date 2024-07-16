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
	StreamID            string     `json:"stream_id"`
	CreatedAt           time.Time  `json:"inserted_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	BackfillCompletedAt *time.Time `json:"backfill_completed_at"`
}

// Add this helper function to format BackfillCompletedAt
func (pr *PostgresReplication) FormatBackfillCompletedAt() string {
	if pr.BackfillCompletedAt == nil {
		return "Not completed"
	}
	return fmt.Sprintf("%s (%s ago)", pr.BackfillCompletedAt.Format(time.RFC3339), time.Since(*pr.BackfillCompletedAt).Round(time.Second))
}

// PostgresReplicationCreate represents the structure for creating a new PostgresReplication
type PostgresReplicationCreate struct {
	SlotName           string `json:"slot_name"`
	PublicationName    string `json:"publication_name"`
	StreamID           string `json:"stream_id"`
	PostgresDatabaseID string `json:"postgres_database_id"`
}

// PostgresReplicationInfo represents the additional info for a PostgresReplication
type PostgresReplicationInfo struct {
	LastCommittedAt *time.Time `json:"last_committed_at"`
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

// FetchPostgresReplicationInfo retrieves information for a specific PostgresReplication from the API
func FetchPostgresReplicationInfo(ctx *context.Context, replicationID string) (*PostgresReplicationWithInfo, error) {
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

// AddPostgresReplication creates a new PostgresReplication
func AddPostgresReplication(ctx *context.Context, replicationData *PostgresReplicationCreate) (*PostgresReplication, error) {
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
		body, _ := io.ReadAll(resp.Body)
		return ParseAPIError(resp.StatusCode, string(body))
	}

	return nil
}
