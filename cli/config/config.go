package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/sequinstream/sequin/cli/context"
)

type PlanResponse struct {
	Changes []Change `json:"changes"`
}

type Change struct {
	ResourceType string      `json:"resource_type"`
	Action       string      `json:"action"`
	Old          interface{} `json:"old,omitempty"`
	New          interface{} `json:"new,omitempty"`
}

type ApplyResponse struct {
	Resources []interface{} `json:"resources"`
}

func Plan(ctx *context.Context, yamlPath string) (*PlanResponse, error) {
	// Read YAML file
	yamlContent, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read YAML file: %w", err)
	}

	// Get server URL
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	// Create request with YAML wrapped in JSON
	requestBody := struct {
		YAML string `json:"yaml"`
	}{
		YAML: string(yamlContent),
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	url := fmt.Sprintf("%s/api/config/plan", serverURL)
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", ctx.ApiToken))

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned error: %s", string(body))
	}

	// Parse response
	var planResp PlanResponse
	if err := json.Unmarshal(body, &planResp); err != nil {
		fmt.Println(string(body))
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &planResp, nil
}

func Apply(ctx *context.Context, yamlPath string) (*ApplyResponse, error) {
	// Read YAML file
	yamlContent, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read YAML file: %w", err)
	}

	// Get server URL
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	// Create request with YAML wrapped in JSON
	requestBody := struct {
		YAML string `json:"yaml"`
	}{
		YAML: string(yamlContent),
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	url := fmt.Sprintf("%s/api/config/apply", serverURL)
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", ctx.ApiToken))

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned error: %s", string(body))
	}

	// Parse response
	var applyResp ApplyResponse
	if err := json.Unmarshal(body, &applyResp); err != nil {
		fmt.Println(string(body))
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &applyResp, nil
}
