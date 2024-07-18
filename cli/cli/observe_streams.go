package cli

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/sequinstream/sequin/cli/api"
	sequinContext "github.com/sequinstream/sequin/cli/context"
)

type StreamState struct {
	streams []api.Stream
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

	output := lipgloss.NewStyle().Bold(true).Render("STREAMS") + "\n\n"

	// Calculate column widths
	nameWidth := s.calculateColumnWidth(width, 0.20, "NAME")
	msgCountWidth := s.calculateColumnWidth(width, 0.15, "MESSAGES")
	storageSizeWidth := s.calculateColumnWidth(width, 0.15, "STORAGE")
	consumerCountWidth := s.calculateColumnWidth(width, 0.15, "CONSUMERS")
	createdAtWidth := s.calculateColumnWidth(width, 0.20, "CREATED AT")
	selectStreamWidth := s.calculateColumnWidth(width, 0.15, "SELECT STREAM")

	// Table header
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("0")). // Black text
		Background(lipgloss.Color("2")). // Green background
		Width(width)

	tableHeader := fmt.Sprintf("%-*s %-*s %-*s %-*s %-*s %-*s",
		nameWidth, "NAME",
		msgCountWidth, "MESSAGES",
		storageSizeWidth, "STORAGE",
		consumerCountWidth, "CONSUMERS",
		createdAtWidth, "CREATED AT",
		selectStreamWidth, "SELECT STREAM")

	output += tableHeaderStyle.Render(tableHeader) + "\n"

	// Stream rows
	for i, stream := range s.streams {
		line := formatStreamLine(stream, nameWidth, msgCountWidth, storageSizeWidth, consumerCountWidth, createdAtWidth)
		style := lipgloss.NewStyle()
		selectStream := ""
		if i == s.cursor {
			style = style.
				Background(lipgloss.Color("117")). // Light blue background
				Foreground(lipgloss.Color("0"))    // Black text
			selectStream = "Press Enter"
		}
		output += style.Render(line+fmt.Sprintf(" %-*s", selectStreamWidth, selectStream)) + "\n"
	}

	return output
}

func (s *StreamState) calculateColumnWidth(totalWidth int, percentage float64, header string) int {
	maxWidth := int(float64(totalWidth) * percentage)
	return max(len(header), maxWidth)
}

func formatStreamLine(stream api.Stream, nameWidth, msgCountWidth, storageSizeWidth, consumerCountWidth, createdAtWidth int) string {
	name := truncateString(stream.Name, nameWidth)
	messageCount := fmt.Sprintf("%d", stream.Stats.MessageCount)
	storageSize := formatBytes(stream.Stats.StorageSize)
	consumerCount := fmt.Sprintf("%d", stream.Stats.ConsumerCount)
	createdAt := stream.CreatedAt.Format("2006-01-02 15:04:05")

	return fmt.Sprintf("%-*s %-*s %-*s %-*s %-*s",
		nameWidth, name,
		msgCountWidth, messageCount,
		storageSizeWidth, storageSize,
		consumerCountWidth, consumerCount,
		createdAtWidth, createdAt)
}

func (s *StreamState) MoveCursor(direction int) {
	s.cursor += direction
	s.cursor = clampValue(s.cursor, 0, len(s.streams)-1)
}

func (s *StreamState) GetSelectedStream() *api.Stream {
	if len(s.streams) == 0 {
		return nil
	}
	return &s.streams[s.cursor]
}
