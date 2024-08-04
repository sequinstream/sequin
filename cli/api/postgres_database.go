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
	Name          string `json:"name"`
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
	Name          string `json:"name"`
	SSL           bool   `json:"ssl,omitempty"`
	Username      string `json:"username"`
	Password      string `json:"password"`
}

type PostgresDatabaseUpdate struct {
	Database      string `json:"database,omitempty"`
	Hostname      string `json:"hostname,omitempty"`
	PoolSize      int    `json:"pool_size,omitempty"`
	Port          int    `json:"port,omitempty"`
	QueueInterval int    `json:"queue_interval,omitempty"`
	QueueTarget   int    `json:"queue_target,omitempty"`
	Name          string `json:"name,omitempty"`
	SSL           bool   `json:"ssl,omitempty"`
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
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

func BuildFetchPostgresDatabaseInfo(ctx *context.Context, databaseID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/databases/%s", serverURL, databaseID), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

func BuildUpdatePostgresDatabase(ctx *context.Context, databaseID string, updateData *PostgresDatabaseUpdate) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(updateData)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("PATCH", fmt.Sprintf("%s/api/databases/%s", serverURL, databaseID), bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

func BuildRemovePostgresDatabase(ctx *context.Context, databaseID string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/databases/%s", serverURL, databaseID), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	return req, nil
}

func FetchPostgresDatabases(ctx *context.Context) ([]PostgresDatabase, error) {
	req, err := BuildFetchPostgresDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("error building fetch postgres databases request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var databasesResponse PostgresDatabasesResponse
	err = json.NewDecoder(resp.Body).Decode(&databasesResponse)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return databasesResponse.Databases, nil
}

func AddPostgresDatabase(ctx *context.Context, databaseData *PostgresDatabaseCreate) (*PostgresDatabase, error) {
	req, err := BuildAddPostgresDatabase(ctx, databaseData)
	if err != nil {
		return nil, fmt.Errorf("error building add postgres database request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var createdDatabase PostgresDatabase
	err = json.NewDecoder(resp.Body).Decode(&createdDatabase)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &createdDatabase, nil
}

func FetchPostgresDatabaseInfo(ctx *context.Context, databaseID string) (*PostgresDatabase, error) {
	req, err := BuildFetchPostgresDatabaseInfo(ctx, databaseID)
	if err != nil {
		return nil, fmt.Errorf("error building fetch postgres database info request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var database PostgresDatabase
	err = json.NewDecoder(resp.Body).Decode(&database)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &database, nil
}

func UpdatePostgresDatabase(ctx *context.Context, databaseID string, updateData *PostgresDatabaseUpdate) (*PostgresDatabase, error) {
	req, err := BuildUpdatePostgresDatabase(ctx, databaseID, updateData)
	if err != nil {
		return nil, fmt.Errorf("error building update postgres database request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, ParseAPIError(resp.StatusCode, string(body))
	}

	var updatedDatabase PostgresDatabase
	err = json.NewDecoder(resp.Body).Decode(&updatedDatabase)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &updatedDatabase, nil
}

func RemovePostgresDatabase(ctx *context.Context, databaseID string) error {
	req, err := BuildRemovePostgresDatabase(ctx, databaseID)
	if err != nil {
		return fmt.Errorf("error building remove postgres database request: %w", err)
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
