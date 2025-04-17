package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInterpolateAction(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "config-command-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test YAML file
	yamlContent := `
# Test YAML file
app:
  name: ${APP_NAME:-test-app}
  port: ${PORT:-8080}
  env: ${ENV:-development}
`
	yamlPath := filepath.Join(tmpDir, "test.yaml")
	outputPath := filepath.Join(tmpDir, "output.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test YAML: %v", err)
	}

	// Set environment variables for tests
	os.Setenv("APP_NAME", "my-custom-app")
	os.Setenv("ENV", "testing")
	defer os.Unsetenv("APP_NAME")
	defer os.Unsetenv("ENV")

	// Helper function to capture stdout
	captureOutput := func(f func() error) (string, error) {
		old := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		err := f()

		w.Close()
		os.Stdout = old

		var buf bytes.Buffer
		io.Copy(&buf, r)
		return buf.String(), err
	}

	t.Run("interpolate to stdout", func(t *testing.T) {
		cmd := &ConfigCommands{yamlPath: yamlPath}

		output, err := captureOutput(func() error {
			return cmd.interpolateAction(nil)
		})

		if err != nil {
			t.Errorf("interpolateAction() error = %v", err)
			return
		}

		// Expected full output
		expectedOutput := `
# Test YAML file
app:
  name: my-custom-app
  port: 8080
  env: testing
`
		// Compare full output
		if output != expectedOutput {
			t.Errorf("Output does not match expected.\nGot:\n%s\nWant:\n%s", output, expectedOutput)
		}
	})

	t.Run("interpolate to file", func(t *testing.T) {
		cmd := &ConfigCommands{
			yamlPath:   yamlPath,
			outputPath: outputPath,
		}

		output, err := captureOutput(func() error {
			return cmd.interpolateAction(nil)
		})

		if err != nil {
			t.Errorf("interpolateAction() error = %v", err)
			return
		}

		// Verify the message about writing to file
		if !strings.Contains(output, "Interpolated YAML written to "+outputPath) {
			t.Errorf("Expected success message for file output, got: %s", output)
		}

		// Check the file content
		content, err := os.ReadFile(outputPath)
		if err != nil {
			t.Fatalf("Failed to read output file: %v", err)
		}

		// Expected file content
		expectedFileContent := `
# Test YAML file
app:
  name: my-custom-app
  port: 8080
  env: testing
`
		// Compare full file content
		if string(content) != expectedFileContent {
			t.Errorf("File content does not match expected.\nGot:\n%s\nWant:\n%s", string(content), expectedFileContent)
		}
	})

	t.Run("file not found", func(t *testing.T) {
		cmd := &ConfigCommands{yamlPath: "nonexistent.yaml"}

		_, err := captureOutput(func() error {
			return cmd.interpolateAction(nil)
		})

		if err == nil {
			t.Error("interpolateAction() expected error for nonexistent file, got nil")
		}
	})
}
