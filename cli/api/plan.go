package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sequinstream/sequin/cli/context"
)

type PlanResponse struct {
	Data []Changeset `json:"data"`
}

type Changeset struct {
	Action  string                 `json:"action"`
	Data    map[string]interface{} `json:"data"`
	Changes map[string]interface{} `json:"changes"`
	Errors  []interface{}         `json:"errors,omitempty"`
}

func BuildPlanYaml(ctx *context.Context, yamlContent string) (*http.Request, error) {
	serverURL, err := context.GetServerURL(ctx)
	if err != nil {
		return nil, err
	}

	payload := map[string]string{
		"yaml": yamlContent,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/config/plan/yaml", serverURL), bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", ctx.ApiToken))

	return req, nil
}

func PlanYaml(ctx *context.Context, yamlContent string) (*PlanResponse, error) {
	req, err := BuildPlanYaml(ctx, yamlContent)
	if err != nil {
		return nil, fmt.Errorf("error building plan yaml request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var planResponse PlanResponse
	// // Debug print response body
	// bodyBytes, err := io.ReadAll(resp.Body)
	// if err != nil {
	// 	return nil, fmt.Errorf("error reading response body: %w", err)
	// }
	// fmt.Printf("Response body: %s\n", string(bodyBytes))

	// Create new reader with the bytes we read
	if err := json.NewDecoder(resp.Body).Decode(&planResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return &planResponse, nil
}