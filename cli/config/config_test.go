package config

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sequinstream/sequin/cli/testutil"
)

func TestProcessEnvVars(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		envVars  map[string]string
		expected string
		wantErr  bool
	}{
		{
			name:     "basic substitution",
			input:    "Value: ${TEST_VAR}",
			envVars:  map[string]string{"TEST_VAR": "hello"},
			expected: "Value: hello",
			wantErr:  false,
		},
		{
			name:     "default value",
			input:    "Value: ${NONEXISTENT_VAR:-default}",
			envVars:  map[string]string{},
			expected: "Value: default",
			wantErr:  false,
		},
		{
			name:     "default empty",
			input:    "Value: ${EMPTY_VAR:-default}",
			envVars:  map[string]string{"EMPTY_VAR": ""},
			expected: "Value: default",
			wantErr:  false,
		},
		{
			name:     "multiple substitutions",
			input:    "${A} ${B:-b} ${C:-c}",
			envVars:  map[string]string{"A": "a", "B": "custom_b"},
			expected: "a custom_b c",
			wantErr:  false,
		},
		{
			name:     "nested vars not supported",
			input:    "${${NESTED}}",
			envVars:  map[string]string{"NESTED": "VAR", "VAR": "value"},
			expected: "${${VAR}}",
			wantErr:  false,
		},
		{
			name:     "alternate syntax forms",
			input:    "${VAR-default} ${VAR:+replacement} ${VAR=default} ${VAR+replacement} $$escaped",
			envVars:  map[string]string{"VAR": "value"},
			expected: "value replacement value replacement $escaped",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables for the test
			for k, v := range tt.envVars {
				os.Setenv(k, v)
				defer os.Unsetenv(k)
			}

			// Call the function
			result, err := applyEnvSubst(tt.input)

			// Check error
			if (err != nil) != tt.wantErr {
				t.Errorf("applyEnvSubst() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// If expecting error, don't check result
			if tt.wantErr {
				return
			}

			// Check result
			if result != tt.expected {
				t.Errorf("applyEnvSubst() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestInterpolate(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "interpolate-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Set environment variables for tests
	os.Setenv("USER", "testuser")
	os.Setenv("PORT", "9000")
	defer os.Unsetenv("USER")
	defer os.Unsetenv("PORT")

	// Create test YAML file
	yamlContent := `
username: ${USER}
port: ${PORT}
host: ${HOST:-localhost}
database: ${DB_NAME:-default_db}
`
	inputPath := filepath.Join(tmpDir, "test.yaml")
	if err := os.WriteFile(inputPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test YAML: %v", err)
	}

	// Define the expected output once, to be used in both tests
	expectedOutput := `
username: testuser
port: "9000"
host: localhost
database: default_db
`

	// Test 1: Output to file
	outputPath := filepath.Join(tmpDir, "output.yaml")
	if err := Interpolate(inputPath, outputPath); err != nil {
		t.Fatalf("Interpolate() error = %v", err)
	}

	// Read the output file
	outputContent, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Check the result using compareYAML
	testutil.CompareYAML(t, string(outputContent), expectedOutput)

	// Test 2: Output to stdout
	// Create a pipe to capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call Interpolate
	if err := Interpolate(inputPath, ""); err != nil {
		t.Fatalf("Interpolate() error = %v", err)
	}

	// Restore stdout and read the captured output
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	stdoutContent := buf.String()

	// Check the result using compareYAML
	testutil.CompareYAML(t, stdoutContent, expectedOutput)

	// Test 3: Error handling - nonexistent input file
	if err := Interpolate("nonexistent.yaml", ""); err == nil {
		t.Errorf("Interpolate() expected error for nonexistent file, got nil")
	}
}
