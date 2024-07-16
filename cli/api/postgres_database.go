package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sequinstream/sequin/cli/context"
)

type PostgresDatabase struct {
	ID            string `json:"id"`
	Database      string `json:"database"`
	Hostname      string `json:"hostname"`
	PoolSize      int    `json:"pool_size"`
	Port          int    `json:"port"`
	QueueInterval int    `json:"queue_interval"`
	QueueTarget   int    `json:"queue_target"`
	Slug          string `json:"slug"`
	SSL           bool   `json:"ssl"`
	Username      string `json:"username"`
	Password      string `json:"password,omitempty"`
}

type PostgresDatabaseCreate struct {
	Database      string `json:"database"`
	Hostname      string `json:"hostname"`
	PoolSize      int    `json:"pool_size,omitempty"`
	Port          int    `json:"port"`
	QueueInterval int    `json:"queue_interval,omitempty"`
	QueueTarget   int    `json:"queue_target,omitempty"`
	Slug          string `json:"slug"`
	SSL           bool   `json:"ssl,omitempty"`
	Username      string `json:"username"`
	Password      string `json:"password"`
}

type PostgresDatabasesResponse struct {
	Databases []PostgresDatabase `json:"data"`
}

func BuildFetchPostgresDatabases(ctx *context.Context) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", serverURL+"/api/databases", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

func FetchPostgresDatabases(ctx *context.Context) ([]PostgresDatabase, error) {
	req, err := BuildFetchPostgresDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("error building fetch databases request: %w", err)
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

	var databasesResponse PostgresDatabasesResponse
	err = json.Unmarshal(body, &databasesResponse)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return databasesResponse.Databases, nil
}

func BuildAddPostgresDatabase(ctx *context.Context, databaseData *PostgresDatabaseCreate) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(databaseData)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", serverURL+"/api/databases", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

func AddPostgresDatabase(ctx *context.Context, databaseData *PostgresDatabaseCreate) (*PostgresDatabase, error) {
	req, err := BuildAddPostgresDatabase(ctx, databaseData)
	if err != nil {
		return nil, fmt.Errorf("error building add database request: %w", err)
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

	var database PostgresDatabase
	err = json.Unmarshal(body, &database)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return &database, nil
}

type SetupReplicationRequest struct {
	DatabaseID      string     `json:"database_id"`
	SlotName        string     `json:"slot_name"`
	PublicationName string     `json:"publication_name"`
	Tables          [][]string `json:"tables"`
}

func SetupReplicationSlotAndPublication(ctx *context.Context, databaseID, slotName, publicationName string, tables [][]string) error {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return err
	}

	setupData := SetupReplicationRequest{
		DatabaseID:      databaseID,
		SlotName:        slotName,
		PublicationName: publicationName,
		Tables:          tables,
	}

	jsonData, err := json.Marshal(setupData)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}
	req, err := http.NewRequest("POST", serverURL+"/api/databases/"+databaseID+"/setup_replication", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error setting up replication: %s", string(body))
	}

	return nil
}

func ListSchemas(ctx *context.Context, databaseID string) ([]string, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", serverURL+"/api/databases/"+databaseID+"/schemas", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error listing schemas: %s", string(body))
	}

	var result struct {
		Schemas []string `json:"schemas"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return result.Schemas, nil
}

func ListTables(ctx *context.Context, databaseID, schema string) ([]string, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", serverURL+"/api/databases/"+databaseID+"/schemas/"+schema+"/tables", nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error listing tables: %s", string(body))
	}

	var result struct {
		Tables []string `json:"tables"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return result.Tables, nil
}
