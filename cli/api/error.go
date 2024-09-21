package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// ValidationError represents an error that occurs during API validation
type ValidationError struct {
	Summary          string                 `json:"summary"`
	ValidationErrors map[string]interface{} `json:"validation_errors"`
	Code             string                 `json:"code"`
}

// NewValidationError creates a new ValidationError
func NewValidationError(summary string, errors map[string]interface{}) *ValidationError {
	return &ValidationError{
		Summary:          summary,
		ValidationErrors: errors,
	}
}

// Error implements the error interface for ValidationError
func (ve *ValidationError) Error() string {
	var parts []string
	if ve.Summary != "" {
		parts = append(parts, ve.Summary)
	}
	if len(ve.ValidationErrors) > 0 {
		for key, value := range ve.ValidationErrors {
			switch v := value.(type) {
			case []interface{}:
				for _, msg := range v {
					parts = append(parts, fmt.Sprintf("%s: %v", key, msg))
				}
			case string:
				parts = append(parts, fmt.Sprintf("%s: %s", key, v))
			default:
				parts = append(parts, fmt.Sprintf("%s: %v", key, v))
			}
		}
	}
	if len(parts) == 0 {
		return "An unknown validation error occurred"
	}
	return strings.Join(parts, "; ")
}

// PrintValidationError prints the validation error to the console
func (ve *ValidationError) PrintValidationError() {
	if ve.Summary != "" {
		fmt.Printf("Validation error: %s\n", ve.Summary)
		fmt.Println()
	}

	if len(ve.ValidationErrors) > 0 {
		errors := make([]string, 0, len(ve.ValidationErrors))
		for field, messages := range ve.ValidationErrors {
			switch v := messages.(type) {
			case []interface{}:
				for _, msg := range v {
					errors = append(errors, fmt.Sprintf("%s: %v", field, msg))
				}
			case string:
				errors = append(errors, fmt.Sprintf("%s: %s", field, v))
			case map[string]interface{}:
				for subField, subMessage := range v {
					errors = append(errors, fmt.Sprintf("%s.%s: %v", field, subField, subMessage))
				}
			}
		}
		fmt.Printf("Validation errors: %s\n", strings.Join(errors, ", "))
	}
}

// APIError represents a generic API error
type APIError struct {
	StatusCode int
	Body       string
}

// NewAPIError creates a new APIError
func NewAPIError(statusCode int, body string) *APIError {
	return &APIError{
		StatusCode: statusCode,
		Body:       body,
	}
}

// Error implements the error interface for APIError
func (ae *APIError) Error() string {
	return fmt.Sprintf("API error (status code %d):\n%s\n", ae.StatusCode, ae.Body)
}

// PrintAPIError prints the API error to the console
func (ae *APIError) PrintAPIError() {
	fmt.Printf("API error (status code %d):\n%s\n", ae.StatusCode, ae.Body)
}

// ParseAPIError determines the type of error and returns the appropriate error struct
func ParseAPIError(statusCode int, body string) error {
	switch statusCode {
	case http.StatusUnprocessableEntity:
		var validationErr ValidationError
		if err := json.Unmarshal([]byte(body), &validationErr); err == nil {
			return &validationErr
		}
	default:
		return NewAPIError(statusCode, body)
	}
	return nil
}
