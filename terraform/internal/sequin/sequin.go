package sequin

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Client represents a Sequin API client
type Client struct {
	Endpoint string
	APIKey   string
}

// NewClient creates a new Sequin client instance
func NewClient(endpoint, apiKey *string) (*Client, error) {
	if endpoint == nil || apiKey == nil {
		return nil, fmt.Errorf("endpoint and apiKey cannot be nil")
	}

	return &Client{
		Endpoint: *endpoint,
		APIKey:   *apiKey,
	}, nil
}


type Database struct {
	ID          string
	Name        string
	Database    string
	Hostname    string
	PoolSize    int
	Port        int
	SSL         bool
	Username      string
	Ipv6          bool
	UseLocalTunnel bool
	Tables        []Table
}

type Table struct {
	Oid int
	Name string
	Columns []Column
}

type Column struct {
	Oid int
	Name string
	Type string
	IsPK bool
}

// DatabaseResponse represents the JSON response from the /api/databases endpoint
type DatabaseResponse struct {
	Data []struct {
		ID             string `json:"id"`
		Database       string `json:"database"`
		Hostname       string `json:"hostname"`
		PoolSize       int    `json:"pool_size"`
		Port           int    `json:"port"`
		SSL            bool   `json:"ssl"`
		Name           string `json:"name"`
		Username       string `json:"username"`
		Ipv6          bool   `json:"ipv6"`
		UseLocalTunnel bool   `json:"use_local_tunnel"`
	} `json:"data"`
}

func (c *Client) GetDatabases() ([]Database, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/databases", c.Endpoint), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.APIKey))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var dbResp DatabaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&dbResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	databases := make([]Database, len(dbResp.Data))
	for i, d := range dbResp.Data {
		databases[i] = Database{
			ID:             d.ID,
			Name:           d.Name,
			Database:       d.Database,
			Hostname:       d.Hostname,
			PoolSize:       d.PoolSize,
			Port:           d.Port,
			SSL:            d.SSL,
			Username:       d.Username,
			Ipv6:          d.Ipv6,
			UseLocalTunnel: d.UseLocalTunnel,
		}
	}

	return databases, nil
}
