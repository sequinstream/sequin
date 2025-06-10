package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
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
  env: ${ENV:-development}
  map:
    name: ${APP_NAME:-test-app}
    port: ${PORT:-8080}
  list:
  - ${RATIO:-3.1416}
  - ${API_KEY:0}
`
	yamlPath := filepath.Join(tmpDir, "test.yaml")
	outputPath := filepath.Join(tmpDir, "output.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test YAML: %v", err)
	}

	// Set environment variables for tests
	os.Setenv("APP_NAME", "my-custom-app")
	os.Setenv("ENV", "testing")
	os.Setenv("API_KEY", "00000000123456789123456789")
	defer os.Unsetenv("APP_NAME")
	defer os.Unsetenv("ENV")
	defer os.Unsetenv("API_KEY")

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

	// Helper function to parse YAML and compare objects
	compareYAML := func(t *testing.T, got, want string) {
		var gotObj, wantObj interface{}
		if err := yaml.Unmarshal([]byte(got), &gotObj); err != nil {
			t.Errorf("Failed to parse got YAML: %v", err)
			return
		}
		if err := yaml.Unmarshal([]byte(want), &wantObj); err != nil {
			t.Errorf("Failed to parse want YAML: %v", err)
			return
		}
		if !reflect.DeepEqual(gotObj, wantObj) {
			t.Errorf("YAML objects do not match.\nGot:\n%s\nWant:\n%s", got, want)
		}
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

		expectedOutput := `
# Test YAML file
app:
  env: testing
  map:
    name: my-custom-app
    port: "8080"
  list:
  - "3.1416"
  - "00000000123456789123456789"
`
		compareYAML(t, output, expectedOutput)
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

		content, err := os.ReadFile(outputPath)
		if err != nil {
			t.Fatalf("Failed to read output file: %v", err)
		}

		expectedFileContent := `
# Test YAML file
app:
  env: testing
  map:
    name: my-custom-app
    port: "8080"
  list:
  - "3.1416"
  - "00000000123456789123456789"
`
		compareYAML(t, string(content), expectedFileContent)
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
