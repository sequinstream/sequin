package api

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	if ve.Summary != "" {
		return ve.Summary
	}
	return "Validation error occurred"
}

// PrintValidationError prints the validation error to the console
func (ve *ValidationError) PrintValidationError() {
	fmt.Printf("Validation error: %s\n", ve.Error())
	for field, message := range ve.ValidationErrors {
		switch v := message.(type) {
		case string:
			fmt.Printf("  %s: %s\n", field, v)
		case map[string]interface{}:
			for subField, subMessage := range v {
				fmt.Printf("  %s.%s: %v\n", field, subField, subMessage)
			}
		}
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
