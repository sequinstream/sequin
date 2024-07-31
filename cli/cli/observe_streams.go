package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/cli/table"
	sequinContext "github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type StreamState struct {
	streams []models.Stream
	config  *Config
	cursor  int
	err     error
}

func NewStreamState(config *Config) *StreamState {
	return &StreamState{
		config: config,
		cursor: 0,
	}
}

func (s *StreamState) FetchStreams(limit int) error {
	ctx, err := sequinContext.LoadContext(s.config.ContextName)
	if err != nil {
		return err
	}

	streams, err := api.FetchStreams(ctx)
	if err != nil {
		return err
	}

	s.streams = streams
	return nil
}

func (s *StreamState) View(width, height int) string {
	if s.err != nil {
		return fmt.Sprintf("Error: %v", s.err)
	}

	if len(s.streams) == 0 {
		message := "No streams available. Create a stream:\n\n"
		codeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("12")) // Blue color for code
		codePart := codeStyle.Render("`sequin stream add`")
		message += codePart

		lines := strings.Split(message, "\n")
		verticalPadding := (height - len(lines)) / 2

		var output strings.Builder
		output.WriteString(strings.Repeat("\n", verticalPadding))

		for _, line := range lines {
			horizontalPadding := (width - lipgloss.Width(line)) / 2
			output.WriteString(fmt.Sprintf("%s%s\n", strings.Repeat(" ", horizontalPadding), line))
		}

		return output.String()
	}

	table := table.NewTable(
		[]table.Column{
			{Name: "NAME", MinWidth: 10, ValueFunc: func(row interface{}) string {
				return row.(models.Stream).Name
			}},
			{Name: "MESSAGES", MinWidth: 8, ValueFunc: func(row interface{}) string {
				return fmt.Sprintf("%d", row.(models.Stream).Stats.MessageCount)
			}},
			{Name: "STORAGE", MinWidth: 7, ValueFunc: func(row interface{}) string {
				return formatBytes(row.(models.Stream).Stats.StorageSize)
			}},
			{Name: "CONSUMERS", MinWidth: 9, ValueFunc: func(row interface{}) string {
				return fmt.Sprintf("%d", row.(models.Stream).Stats.ConsumerCount)
			}},
			{Name: "CREATED AT", MinWidth: 20, ValueFunc: func(row interface{}) string {
				return row.(models.Stream).CreatedAt.Format(time.RFC3339)
			}},
		},
		streamInterfaceSlice(s.streams),
		width,
	)

	table.SetActionColumn("SELECT STREAM", func(row interface{}) string {
		return "Press Enter"
	})
	table.SelectedIndex = s.cursor
	table.ActionOnSelect = true

	return "Select a stream to observe it\n\n" + table.Render()
}

func streamInterfaceSlice(streams []models.Stream) []interface{} {
	result := make([]interface{}, len(streams))
	for i, v := range streams {
		result[i] = v
	}
	return result
}

func (s *StreamState) MoveCursor(direction int) {
	s.cursor += direction
	s.cursor = clampValue(s.cursor, 0, len(s.streams)-1)
}

func (s *StreamState) GetSelectedStream() *models.Stream {
	if len(s.streams) == 0 {
		return nil
	}
	return &s.streams[s.cursor]
}
