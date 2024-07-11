package cli

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"moul.io/http2curl"
)

func addCheat(name string, cmd *fisk.CmdClause) {
	cmd.CheatFile(fs, name, fmt.Sprintf("cheats/%s.md", name))
}

func newTableWriter(format string, a ...any) *tbl {
	tbl := &tbl{
		writer: table.NewWriter(),
	}

	tbl.writer.SetStyle(styles["rounded"])

	tbl.writer.Style().Title.Align = text.AlignCenter
	tbl.writer.Style().Format.Header = text.FormatDefault
	tbl.writer.Style().Format.Footer = text.FormatDefault

	if format != "" {
		tbl.writer.SetTitle(fmt.Sprintf(format, a...))
	}

	return tbl
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
