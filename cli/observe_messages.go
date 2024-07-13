package cli

import (
	"fmt"
	"strings"
	"time"

	"sequin-cli/api"
	"sequin-cli/context"

	"github.com/charmbracelet/lipgloss"
)

type Message struct {
	messages   []api.Message
	config     *Config
	cursor     int
	showDetail bool
	filter     string
}

func NewMessage(config *Config) *Message {
	return &Message{
		config:     config,
		cursor:     0,
		showDetail: false,
		filter:     ">",
	}
}

func (m *Message) FetchMessages(limit int, filter string) error {
	ctx, err := context.LoadContext(m.config.ContextName)
	if err != nil {
		return err
	}

	m.filter = filter
	messages, err := api.ListStreamMessages(ctx, "default", limit, "seq_desc", filter)
	if err != nil {
		return err
	}

	m.messages = messages
	return nil
}

func (m *Message) View(width, height int) string {
	if m.showDetail {
		return m.detailView(width, height)
	}

	seqWidth := 10
	keyWidth := max(10, width/3)
	createdWidth := 22
	dataWidth := max(10, width-seqWidth-keyWidth-createdWidth-8)

	output := fmt.Sprintf("%-*s | %-*s | %-*s | %-*s\n", seqWidth, "Seq", keyWidth, "Key", createdWidth, "Created", dataWidth, "Data")
	output += strings.Repeat("-", width) + "\n"

	for i, msg := range m.messages {
		seq := fmt.Sprintf("%d", msg.Seq)
		key := truncateString(msg.Key, keyWidth)
		created := msg.CreatedAt.Format(time.RFC3339)
		data := truncateString(msg.Data, dataWidth)

		style := lipgloss.NewStyle()
		if i == m.cursor {
			style = style.Background(lipgloss.Color("25")).Foreground(lipgloss.Color("231"))
		}
		line := fmt.Sprintf("%-*s | %-*s | %-*s | %-*s",
			seqWidth, seq,
			keyWidth, key,
			createdWidth, created,
			dataWidth, data)
		output += style.Render(line) + "\n"
	}

	for i := 0; i < height-len(m.messages)-3; i++ {
		output += "\n"
	}

	return output
}

func (m *Message) detailView(width, height int) string {
	msg := m.messages[m.cursor]
	output := lipgloss.NewStyle().Bold(true).Render("Message Detail")
	output += "\n\n"
	output += fmt.Sprintf("Seq:     %d\n", msg.Seq)
	output += fmt.Sprintf("Key:     %s\n", msg.Key)
	output += fmt.Sprintf("Created: %s\n", msg.CreatedAt.Format(time.RFC3339))

	// Limit data to a maximum number of lines
	maxDataLines := height - 10
	dataLines := strings.Split(msg.Data, "\n")
	if len(dataLines) > maxDataLines {
		dataLines = dataLines[:maxDataLines]
		dataLines = append(dataLines, "...")
	}
	output += fmt.Sprintf("Data:\n%s\n", strings.Join(dataLines, "\n"))

	// Pad the output to fill the available height
	lines := strings.Count(output, "\n")
	for i := 0; i < height-lines-1; i++ {
		output += "\n"
	}

	return output
}

func (m *Message) ToggleDetail() {
	m.showDetail = !m.showDetail
}

func (m *Message) MoveCursor(direction int) {
	m.cursor += direction
	if m.cursor < 0 {
		m.cursor = 0
	} else if m.cursor >= len(m.messages) {
		m.cursor = len(m.messages) - 1
	}
}

func (m *Message) IsDetailView() bool {
	return m.showDetail
}

func truncateString(s string, maxLen int) string {
	// Handle negative or zero maxLen
	if maxLen <= 0 {
		return ""
	}

	// Trim to the first newline
	if idx := strings.Index(s, "\n"); idx != -1 {
		s = s[:idx]
	}

	// Handle cases where maxLen is too small
	if maxLen <= 3 {
		return strings.Repeat(".", maxLen)
	}

	// Truncate if still longer than maxLen
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
