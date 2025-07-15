package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/a8m/envsubst"
	"github.com/goccy/go-yaml"
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

type ProcessingContext struct {
	YamlPath    string
	SearchPaths []string
}

var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		DisableKeepAlives:     false,
	},
}

// NewProcessingContext creates a new processing context with search paths set up
func NewProcessingContext(yamlPath string) (*ProcessingContext, error) {
	if yamlPath == "" {
		return nil, fmt.Errorf("yaml path cannot be empty")
	}

	// Handle stdin case
	if yamlPath == "-" {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("failed to get current directory: %w", err)
		}
		return &ProcessingContext{
			YamlPath:    yamlPath,
			SearchPaths: []string{cwd},
		}, nil
	}

	// Clean the path
	yamlPath = filepath.Clean(yamlPath)

	// For non-stdin input, only use the directory of the YAML file as the search path
	yamlDir := filepath.Dir(yamlPath)
	absYamlDir, err := filepath.Abs(yamlDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve yaml directory: %w", err)
	}

	return &ProcessingContext{
		YamlPath:    yamlPath,
		SearchPaths: []string{absYamlDir},
	}, nil
}

// resolveFilePath resolves a file path by trying each search path in order
func resolveFilePath(pctx *ProcessingContext, filePath string) (string, error) {
	// If it's already an absolute path, use it as-is
	if filepath.IsAbs(filePath) {
		if _, err := os.Stat(filePath); err != nil {
			return "", fmt.Errorf("file not found: %s", filePath)
		}
		return filePath, nil
	}

	// Try each search path in order
	var triedPaths []string
	for _, searchPath := range pctx.SearchPaths {
		candidatePath := filepath.Join(searchPath, filePath)
		triedPaths = append(triedPaths, candidatePath)

		if _, err := os.Stat(candidatePath); err == nil {
			return filepath.Abs(candidatePath)
		}
	}

	// All attempts failed
	return "", fmt.Errorf("file not found: tried %s", strings.Join(triedPaths, ", "))
}

// Apply envsubst to all string values except for those under the "code" key
func applyEnvSubst(node interface{}) (interface{}, error) {
	return applyEnvSubstInternal(node, false)
}

func applyEnvSubstInternal(node interface{}, skip bool) (interface{}, error) {
	switch v := node.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, value := range v {
			// If the key is "code", set skip=true for its value
			skipSubst := skip || (key == "code")
			processed, err := applyEnvSubstInternal(value, skipSubst)
			if err != nil {
				return nil, err
			}
			result[key] = processed
		}
		return result, nil

	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			processed, err := applyEnvSubstInternal(item, skip)
			if err != nil {
				return nil, err
			}
			result[i] = processed
		}
		return result, nil

	case string:
		if skip {
			return v, nil
		}
		substituted, err := envsubst.String(v)
		if err != nil {
			return nil, fmt.Errorf("environment variable substitution failed for '%s': %w", v, err)
		}
		return substituted, nil

	default:
		return v, nil
	}
}

func processYAML(pctx *ProcessingContext, yamlContent []byte) ([]byte, error) {
	var yamlData interface{}
	if err := yaml.Unmarshal(yamlContent, &yamlData); err != nil {
		return nil, fmt.Errorf("Failed to parse YAML content: %w", err)
	}

	with_subst, err := applyEnvSubst(yamlData)
	if err != nil {
		return nil, fmt.Errorf("environment variable substitution failed: %w", err)
	}

	with_files, err := processFunctions(pctx, with_subst)
	if err != nil {
		return nil, fmt.Errorf("function parsing failed: %w", err)
	}

	processed, err := yaml.Marshal(with_files)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode YAML: %w", err)
	}

	return processed, nil
}

func processFunctions(pctx *ProcessingContext, node interface{}) (interface{}, error) {
	topLevel, ok := node.(map[string]interface{})
	if !ok {
		return node, nil
	}

	result := make(map[string]interface{})

	for key, value := range topLevel {
		if key == "functions" {
			processed, err := processFunctionsList(pctx, value)
			if err != nil {
				return nil, err
			}
			result[key] = processed
		} else {
			result[key] = value
		}
	}

	return result, nil
}

func processFunctionsList(pctx *ProcessingContext, node interface{}) (interface{}, error) {
	switch v := node.(type) {
	case map[string]interface{}:
		return processFileInFunction(pctx, v)

	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			if funcObj, ok := item.(map[string]interface{}); ok {
				processed, err := processFileInFunction(pctx, funcObj)
				if err != nil {
					return nil, err
				}
				result[i] = processed
			} else {
				result[i] = item
			}
		}
		return result, nil

	default:
		return v, nil
	}
}

func processFileInFunction(pctx *ProcessingContext, funcObj map[string]interface{}) (map[string]interface{}, error) {
	if filePathRaw, hasFile := funcObj["file"]; hasFile {
		if filePath, ok := filePathRaw.(string); ok {
			resolvedPath, err := resolveFilePath(pctx, filePath)
			if err != nil {
				return nil, fmt.Errorf("error resolving file path %s: %w", filePath, err)
			}

			content, err := os.ReadFile(resolvedPath)
			if err != nil {
				return nil, fmt.Errorf("error reading file %s: %w", resolvedPath, err)
			}

			result := make(map[string]interface{})
			for k, val := range funcObj {
				if k != "file" {
					result[k] = val
				}
			}
			result["code"] = string(content)
			return result, nil
		}
	}

	return funcObj, nil
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

	pctx, err := NewProcessingContext(inputPath)
	if err != nil {
		return err
	}
	processed, err := processYAML(pctx, yamlContent)
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

	// Process the summary field first if it exists and is not nil
	if summary, ok := errorData["summary"].(string); ok && summary != "" {
		result.WriteString(summary)
		delete(errorData, "summary")
	}

	// Process any additional fields
	for key, value := range errorData {
		// Skip nil values
		if value == nil {
			continue
		}

		// Add newline if we already have content
		if result.Len() > 0 {
			result.WriteString("\n")
		}

		// Format the value based on its type
		switch v := value.(type) {
		case map[string]interface{}:
			result.WriteString(fmt.Sprintf("%s:", key))
			formatMap(&result, v, 1)
		case []interface{}:
			result.WriteString(fmt.Sprintf("%s:", key))
			formatSlice(&result, v, 1)
		default:
			result.WriteString(fmt.Sprintf("%s: %v", key, value))
		}
	}

	return result.String()
}

// formatMap formats a map with proper indentation
func formatMap(sb *strings.Builder, m map[string]interface{}, indent int) {
	for key, value := range m {
		sb.WriteString(fmt.Sprintf("\n%s%s:", strings.Repeat("  ", indent), key))
		switch v := value.(type) {
		case map[string]interface{}:
			formatMap(sb, v, indent+1)
		case []interface{}:
			formatSlice(sb, v, indent+1)
		default:
			sb.WriteString(fmt.Sprintf(" %v", value))
		}
	}
}

// formatSlice formats a slice with proper indentation
func formatSlice(sb *strings.Builder, s []interface{}, indent int) {
	for _, value := range s {
		switch v := value.(type) {
		case map[string]interface{}:
			sb.WriteString("\n" + strings.Repeat("  ", indent))
			formatMap(sb, v, indent+1)
		default:
			sb.WriteString(fmt.Sprintf("\n%s- %v", strings.Repeat("  ", indent), value))
		}
	}
}

func Plan(ctx *context.Context, yamlPath string) (*PlanResponse, error) {
	// Read YAML file
	yamlContent, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read YAML file: %w", err)
	}

	// Process YAML content
	pctx, err := NewProcessingContext(yamlPath)
	if err != nil {
		return nil, err
	}
	yamlContent, err = processYAML(pctx, yamlContent)
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
	resp, err := httpClient.Do(req)
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
	pctx, err := NewProcessingContext(yamlPath)
	if err != nil {
		return nil, err
	}
	yamlContent, err = processYAML(pctx, yamlContent)
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
	resp, err := httpClient.Do(req)
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
	resp, err := httpClient.Do(req)
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
