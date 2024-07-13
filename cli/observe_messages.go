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
		return m.detailView(height)
	}
	return m.listView(width, height)
}

func (m *Message) listView(width, height int) string {
	seqWidth := 10
	keyWidth := max(10, width/3)
	createdWidth := 22
	dataWidth := max(10, width-seqWidth-keyWidth-createdWidth-8)

	output := fmt.Sprintf("%-*s | %-*s | %-*s | %-*s\n", seqWidth, "Seq", keyWidth, "Key", createdWidth, "Created", dataWidth, "Data")
	output += strings.Repeat("-", width) + "\n"

	for i, msg := range m.messages {
		line := formatMessageLine(msg, seqWidth, keyWidth, createdWidth, dataWidth)
		style := lipgloss.NewStyle()
		if i == m.cursor {
			style = style.Background(lipgloss.Color("25")).Foreground(lipgloss.Color("231"))
		}
		output += style.Render(line) + "\n"
	}

	// Pad the output
	for i := 0; i < height-len(m.messages)-3; i++ {
		output += "\n"
	}

	return output
}

func formatMessageLine(msg api.Message, seqWidth, keyWidth, createdWidth, dataWidth int) string {
	seq := fmt.Sprintf("%d", msg.Seq)
	key := truncateString(msg.Key, keyWidth)
	created := msg.CreatedAt.Format(time.RFC3339)
	data := truncateString(msg.Data, dataWidth)

	return fmt.Sprintf("%-*s | %-*s | %-*s | %-*s",
		seqWidth, seq,
		keyWidth, key,
		createdWidth, created,
		dataWidth, data)
}

func (m *Message) detailView(height int) string {
	msg := m.messages[m.cursor]
	output := lipgloss.NewStyle().Bold(true).Render("Message Detail")
	output += "\n\n"
	output += fmt.Sprintf("Seq:     %d\n", msg.Seq)
	output += fmt.Sprintf("Key:     %s\n", msg.Key)
	output += fmt.Sprintf("Created: %s\n", msg.CreatedAt.Format(time.RFC3339))

	output += formatDetailData(msg.Data, height-10)

	// Pad the output
	lines := strings.Count(output, "\n")
	for i := 0; i < height-lines-1; i++ {
		output += "\n"
	}

	return output
}

func formatDetailData(data string, maxLines int) string {
	dataLines := strings.Split(data, "\n")
	if len(dataLines) > maxLines {
		dataLines = dataLines[:maxLines]
		dataLines = append(dataLines, "...")
	}
	return fmt.Sprintf("Data:\n%s\n", strings.Join(dataLines, "\n"))
}

func (m *Message) ToggleDetail() {
	m.showDetail = !m.showDetail
}

func (m *Message) MoveCursor(direction int) {
	m.cursor += direction
	m.cursor = clampValue(m.cursor, 0, len(m.messages)-1)
}

func (m *Message) IsDetailView() bool {
	return m.showDetail
}

func truncateString(s string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	if idx := strings.Index(s, "\n"); idx != -1 {
		s = s[:idx]
	}
	if maxLen <= 3 {
		return strings.Repeat(".", maxLen)
	}
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

func clampValue(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
