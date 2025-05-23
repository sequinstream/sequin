package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/a8m/envsubst"
	"github.com/sequinstream/sequin/cli/context"
)

type PlanResponse struct {
	Changes []Change `json:"changes"`
	Actions []Action `json:"actions"`
}

type Change struct {
	ResourceType string      `json:"resource_type"`
	Action       string      `json:"action"`
	Old          interface{} `json:"old,omitempty"`
	New          interface{} `json:"new,omitempty"`
}

type Action struct {
	Description string `json:"description"`
}

type ApplyResponse struct {
	Resources []interface{} `json:"resources"`
}

type ExportResponse struct {
	YAML string `json:"yaml"`
}

// processEnvVars replaces environment variables in the YAML content using envsubst library
func processEnvVars(yamlContent []byte) ([]byte, error) {
	processed, err := envsubst.Bytes(yamlContent)
	if err != nil {
		return nil, fmt.Errorf("failed to process environment variables: %w", err)
	}
	return processed, nil
}

// Interpolate reads a YAML file, processes environment variables, and outputs the result
func Interpolate(inputPath, outputPath string) error {
	var yamlContent []byte
	var err error

	// Read YAML content from stdin or file
	if inputPath == "-" {
		// Read from stdin
		yamlContent, err = io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("failed to read from stdin: %w", err)
		}
	} else {
		// Read from file
		yamlContent, err = os.ReadFile(inputPath)
		if err != nil {
			return fmt.Errorf("failed to read YAML file: %w", err)
		}
	}

	// Process environment variables
	processed, err := processEnvVars(yamlContent)
	if err != nil {
		return err
	}

	// Write output
	if outputPath == "" {
		// Write to stdout if no output path specified
		_, err = os.Stdout.Write(processed)
	} else {
		// Write to the specified file
		err = os.WriteFile(outputPath, processed, 0644)
	}

	if err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}

	return nil
}

// PrettyPrintServerError formats a server error response
func PrettyPrintServerError(errorBody string) string {
	// Try to parse the error as JSON
	var errorData map[string]interface{}
	if err := json.Unmarshal([]byte(errorBody), &errorData); err != nil {
		// If it's not valid JSON, return the original error
		return errorBody
	}

	var result strings.Builder

	// Process the summary field if it exists
	if summary, ok := errorData["summary"].(string); ok {
		result.WriteString(summary)
		delete(errorData, "summary")
	}

	// Process any additional fields
	for key, value := range errorData {
		result.WriteString(fmt.Sprintf("\n%s: %v", key, value))
	}

	return result.String()
}

func Plan(ctx *context.Context, yamlPath string) (*PlanResponse, error) {
	// Read YAML file
	yamlContent, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read YAML file: %w", err)
	}

	// Process environment variables
	yamlContent, err = processEnvVars(yamlContent)
	if err != nil {
		return nil, err
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
		errorMsg := PrettyPrintServerError(string(body))
		return nil, fmt.Errorf("server returned error: %s", errorMsg)
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

	// Process environment variables
	yamlContent, err = processEnvVars(yamlContent)
	if err != nil {
		return nil, err
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
		errorMsg := PrettyPrintServerError(string(body))
		return nil, fmt.Errorf("server returned error: %s", errorMsg)
	}

	// Parse response
	var applyResp ApplyResponse
	if err := json.Unmarshal(body, &applyResp); err != nil {
		fmt.Println(string(body))
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &applyResp, nil
}

func Export(ctx *context.Context, showSensitive bool) (*ExportResponse, error) {
	// Get server URL
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/config/export?show-sensitive=%t", serverURL, showSensitive)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

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
		errorMsg := PrettyPrintServerError(string(body))
		return nil, fmt.Errorf("server returned error: %s", errorMsg)
	}

	// Parse response
	var exportResp ExportResponse
	if err := json.Unmarshal(body, &exportResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &exportResp, nil
}
