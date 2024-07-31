package cli

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"moul.io/http2curl"
)

func loadingSpinner() string {
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	return spinner[int(time.Now().UnixNano()/100000000)%len(spinner)] + " Loading...\n"
}

func loadingSpinnerNoText() string {
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	return spinner[int(time.Now().UnixNano()/100000000)%len(spinner)]
}

func addCheat(name string, cmd *fisk.CmdClause) {
	cmd.CheatFile(fs, name, fmt.Sprintf("cheats/%s.md", name))
}

func formatBytes(bytes int) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func askConfirmation(prompt string, dflt bool) (bool, error) {
	ans := dflt

	err := survey.AskOne(&survey.Confirm{
		Message: prompt,
		Default: dflt,
	}, &ans)

	return ans, err
}

func promptForInt(message string, value *int) error {
	var strValue string
	err := survey.AskOne(&survey.Input{
		Message: message,
	}, &strValue)
	if err != nil {
		return fmt.Errorf("failed to get user input: %w", err)
	}
	if strValue != "" {
		intValue, err := strconv.Atoi(strValue)
		if err != nil {
			return fmt.Errorf("invalid integer value: %w", err)
		}
		*value = intValue
	}
	return nil
}

func formatCurl(req *http.Request) (string, error) {
	curl, err := http2curl.GetCurlCommand(req)
	if err != nil {
		return "", fmt.Errorf("error generating curl command: %w", err)
	}
	return curl.String(), nil
}

// func max(a, b int) int {
// 	if a > b {
// 		return a
// 	}
// 	return b
// }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func clampValue(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
