package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sequinstream/sequin/cli/testutil"
)

func TestInterpolateAction(t *testing.T) {
	// Create a nested temporary directory structure for test files
	parentDir, err := os.MkdirTemp("", "config-command-test-parent")
	if err != nil {
		t.Fatalf("Failed to create parent temp dir: %v", err)
	}
	defer os.RemoveAll(parentDir)

	// We create also a child directory to test the `..` relative path without messing with files outside of the test directory
	tmpDir, err := os.MkdirTemp(parentDir, "config-command-test-child")
	if err != nil {
		t.Fatalf("Failed to create child temp dir: %v", err)
	}

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
		testutil.CompareYAML(t, output, expectedOutput)
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
		testutil.CompareYAML(t, string(content), expectedFileContent)
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

	// Helper function to indent each line of a string
	indentLines := func(s string, indent int) string {
		lines := strings.Split(s, "\n")
		indentStr := strings.Repeat(" ", indent)
		for i, line := range lines {
			lines[i] = indentStr + line
		}
		return strings.Join(lines, "\n")
	}

	t.Run("interpolate function code", func(t *testing.T) {
		codeContent := `def transform(action, record, changes, metadata) do
  %{
    id: record["id"],
    action: action
  }
end`

		// Base YAML template that will be used for all tests
		yamlTemplate := `
# Test YAML file with function
functions:
  - name: "my-transform-function"
    description: "Extract ID and action"
    type: "transform"
    file: "%s"
`

		// Helper function to create test files
		createTestFiles := func(t *testing.T, codePath string) error {
			// Create directory if it doesn't exist
			dir := filepath.Dir(codePath)
			if dir != "." {
				if err := os.MkdirAll(dir, 0755); err != nil {
					return fmt.Errorf("failed to create directory: %v", err)
				}
			}
			// Write the code file
			return os.WriteFile(codePath, []byte(codeContent), 0644)
		}

		testCases := []struct {
			name        string
			filePath    string // Path to use in YAML
			codePath    string // Actual path where code should be written
			cleanup     func()
			useStdin    bool   // Whether to use STDIN for YAML input
		}{
			{
				name:     "from current directory with direct relative path",
				filePath: "transform.ex",
				codePath: filepath.Join(tmpDir, "transform.ex"),
			},
			{
				name:     "from current directory with dot relative path",
				filePath: "./transform.ex",
				codePath: filepath.Join(tmpDir, "transform.ex"),
			},
			{
				name:     "from subdirectory with relative path",
				filePath: filepath.Join("functions", "transform.ex"),
				codePath: filepath.Join(tmpDir, "functions", "transform.ex"),
			},
			{
				name:     "from sibling directory with relative path",
				filePath: filepath.Join("..", "sibling-functions", "transform.ex"),
				codePath: filepath.Join(parentDir, "sibling-functions", "transform.ex"),
			},
			{
				name:     "from any directory using absolute path",
				filePath: filepath.Join(parentDir, "sibling-functions", "transform.ex"),
				codePath: filepath.Join(parentDir, "sibling-functions", "transform.ex"),
			},
			{
				name:     "from stdin with relative path",
				filePath: "transform.ex",
				codePath: filepath.Join(tmpDir, "transform.ex"),
				useStdin: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Create the code file
				if err := createTestFiles(t, tc.codePath); err != nil {
					t.Fatalf("Failed to create test files: %v", err)
				}

				// Create YAML file with the appropriate path
				yamlContent := fmt.Sprintf(yamlTemplate, tc.filePath)
				yamlPath := filepath.Join(tmpDir, "test.yaml")
				outputPath := filepath.Join(tmpDir, "output.yaml")

				var cmd *ConfigCommands
				if tc.useStdin {
					// Save current directory and change to temp directory
					originalDir, err := os.Getwd()
					if err != nil {
						t.Fatalf("Failed to get current directory: %v", err)
					}
					defer os.Chdir(originalDir)

					if err := os.Chdir(tmpDir); err != nil {
						t.Fatalf("Failed to change to temp directory: %v", err)
					}

					// Create a pipe to simulate stdin
					oldStdin := os.Stdin
					r, w, _ := os.Pipe()
					os.Stdin = r
					defer func() {
						os.Stdin = oldStdin
						r.Close()
					}()

					// Write YAML content to the pipe
					go func() {
						w.Write([]byte(yamlContent))
						w.Close()
					}()

					cmd = &ConfigCommands{
						yamlPath:   "-",
						outputPath: outputPath,
					}
				} else {
					if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
						t.Fatalf("Failed to write test YAML: %v", err)
					}

					cmd = &ConfigCommands{
						yamlPath:   yamlPath,
						outputPath: outputPath,
					}
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

				expectedFileContent := fmt.Sprintf(`
# Test YAML file with function
functions:
  - name: "my-transform-function"
    description: "Extract ID and action"
    type: "transform"
    code: |-
%s
`, indentLines(codeContent, 6))
				testutil.CompareYAML(t, string(content), expectedFileContent)
			})
		}
	})

	t.Run("function file not found", func(t *testing.T) {
		// Create test YAML file with non-existent function file
		yamlContent := `
# Test YAML file with missing function file
functions:
  - name: "my-transform-function"
    description: "Extract ID and action"
    type: "transform"
    file: "nonexistent.ex"
`
		yamlPath := filepath.Join(tmpDir, "test.yaml")
		if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
			t.Fatalf("Failed to write test YAML: %v", err)
		}

		cmd := &ConfigCommands{yamlPath: yamlPath}

		_, err := captureOutput(func() error {
			return cmd.interpolateAction(nil)
		})

		if err == nil {
			t.Error("interpolateAction() expected error for nonexistent function file, got nil")
		}
	})
}
