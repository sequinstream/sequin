package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/goccy/go-yaml"
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

// Apply envsubst to all string values everywhere
func applyEnvSubst(node interface{}) interface{} {
	switch v := node.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, value := range v {
			result[key] = applyEnvSubst(value)
		}
		return result

	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = applyEnvSubst(item)
		}
		return result

	case string:
		substituted, err := envsubst.String(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: envsubst error: %v\n", err)
			return v
		}
		return substituted

	default:
		return v
	}
}

// Process file substitutions only within top-level "functions"
func processFunctions(node interface{}) interface{} {
	topLevel, ok := node.(map[string]interface{})
	if !ok {
		return node // Not a map at top level, return as-is
	}

	result := make(map[string]interface{})

	for key, value := range topLevel {
		if key == "functions" {
			// Process the functions list/object
			result[key] = processFunctionsList(value)
		} else {
			// Copy everything else unchanged
			result[key] = value
		}
	}

	return result
}

// Process each function object in the functions collection
func processFunctionsList(node interface{}) interface{} {
	switch v := node.(type) {
	case map[string]interface{}:
		// Single function object
		return processFileInFunction(v)

	case []interface{}:
		// Array of function objects
		result := make([]interface{}, len(v))
		for i, item := range v {
			if funcObj, ok := item.(map[string]interface{}); ok {
				result[i] = processFileInFunction(funcObj)
			} else {
				result[i] = item
			}
		}
		return result

	default:
		return v
	}
}

// Handle file: -> code: transformation for a single function object
func processFileInFunction(funcObj map[string]interface{}) map[string]interface{} {
	if filePathRaw, hasFile := funcObj["file"]; hasFile {
		if filePath, ok := filePathRaw.(string); ok {
			fmt.Printf("Processing file reference: %s\n", filePath)

			content, err := os.ReadFile(filePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error reading file %s: %v\n", filePath, err)
				return funcObj // Keep original on error
			}

			// Copy everything except "file", add "code"
			result := make(map[string]interface{})
			for k, val := range funcObj {
				if k != "file" {
					result[k] = val
				}
			}
			result["code"] = string(content)
			return result
		}
	}

	// No file key or not a string, return unchanged
	return funcObj
}

// processYAML applies environment variable substitution and file processing to YAML content
func processYAML(yamlContent []byte) ([]byte, error) {
	var yamlData interface{}
	if err := yaml.Unmarshal(yamlContent, &yamlData); err != nil {
		return nil, fmt.Errorf("Failed to parse YAML content: %w", err)
	}

	with_subst := applyEnvSubst(yamlData)
	with_files := processFunctions(with_subst)
	final := with_files

	processed, err := yaml.Marshal(final)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode YAML: %w", err)
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

	processed, err := processYAML(yamlContent)
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

	// Process YAML content
	yamlContent, err = processYAML(yamlContent)
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

	// Process YAML content
	yamlContent, err = processYAML(yamlContent)
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

