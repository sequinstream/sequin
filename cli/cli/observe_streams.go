package cli

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/sequinstream/sequin/cli/api"
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

	output := lipgloss.NewStyle().Bold(true).Render("Select a stream to observe it")
	output += "\n\n"

	// Calculate column widths
	nameWidth := s.calculateNameWidth()
	msgCountWidth := s.calculateMsgCountWidth()
	storageSizeWidth := s.calculateStorageSizeWidth()
	consumerCountWidth := s.calculateConsumerCountWidth()
	createdAtWidth := s.calculateCreatedAtWidth()

	// Calculate the remaining width for the "SELECT STREAM" column
	usedWidth := nameWidth + msgCountWidth + storageSizeWidth + consumerCountWidth + createdAtWidth + 5 // 5 spaces between columns
	selectStreamWidth := width - usedWidth

	// Ensure minimum width for "SELECT STREAM" column
	if selectStreamWidth < len("SELECT STREAM") {
		// Redistribute width if necessary
		excess := len("SELECT STREAM") - selectStreamWidth
		nameWidth -= excess / 5
		msgCountWidth -= excess / 5
		storageSizeWidth -= excess / 5
		consumerCountWidth -= excess / 5
		createdAtWidth -= excess / 5
		selectStreamWidth = len("SELECT STREAM")
	}

	// Table header
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("0")).   // Black text
		Background(lipgloss.Color("252")). // Light grey background
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
				Background(lipgloss.Color("57")). // Purple background
				Foreground(lipgloss.Color("255")) // White text
			selectStream = "Press Enter"
		}
		output += style.Render(line+fmt.Sprintf(" %-*s", selectStreamWidth, selectStream)) + "\n"
	}

	return output
}

func (s *StreamState) calculateNameWidth() int {
	maxWidth := len("NAME")
	for _, stream := range s.streams {
		if len(stream.Name) > maxWidth {
			maxWidth = len(stream.Name)
		}
	}
	return maxWidth
}

func (s *StreamState) calculateMsgCountWidth() int {
	maxWidth := len("MESSAGES")
	for _, stream := range s.streams {
		width := len(fmt.Sprintf("%d", stream.Stats.MessageCount))
		if width > maxWidth {
			maxWidth = width
		}
	}
	return maxWidth
}

func (s *StreamState) calculateStorageSizeWidth() int {
	maxWidth := len("STORAGE")
	for _, stream := range s.streams {
		width := len(formatBytes(stream.Stats.StorageSize))
		if width > maxWidth {
			maxWidth = width
		}
	}
	return maxWidth
}

func (s *StreamState) calculateConsumerCountWidth() int {
	maxWidth := len("CONSUMERS")
	for _, stream := range s.streams {
		width := len(fmt.Sprintf("%d", stream.Stats.ConsumerCount))
		if width > maxWidth {
			maxWidth = width
		}
	}
	return maxWidth
}

func (s *StreamState) calculateCreatedAtWidth() int {
	return len("2006-01-02 15:04:05") // Fixed width for the date format
}

func formatStreamLine(stream models.Stream, nameWidth, msgCountWidth, storageSizeWidth, consumerCountWidth, createdAtWidth int) string {
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

func (s *StreamState) GetSelectedStream() *models.Stream {
	if len(s.streams) == 0 {
		return nil
	}
	return &s.streams[s.cursor]
}
