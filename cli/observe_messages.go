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
}

func NewMessage(config *Config) *Message {
	return &Message{
		config:     config,
		cursor:     0,
		showDetail: false,
	}
}

func (m *Message) FetchMessages(limit int) error {
	ctx, err := context.LoadContext(m.config.ContextName)
	if err != nil {
		return err
	}

	messages, err := api.ListStreamMessages(ctx, "default", limit, "seq_desc", "")
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
	keyWidth := 20
	createdWidth := 25
	dataWidth := width - seqWidth - keyWidth - createdWidth - 8

	output := fmt.Sprintf("\n%-*s | %-*s | %-*s | %-*s\n", seqWidth, "Seq", keyWidth, "Key", createdWidth, "Created", dataWidth, "Data")
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
	output := lipgloss.NewStyle().Bold(true).Render("Message Detail\n\n")
	output += fmt.Sprintf("Seq:     %d\n", msg.Seq)
	output += fmt.Sprintf("Key:     %s\n", msg.Key)
	output += fmt.Sprintf("Created: %s\n", msg.CreatedAt.Format(time.RFC3339))
	output += fmt.Sprintf("Data:    %s\n", msg.Data)

	// Pad the output to fill the available height
	lines := strings.Count(output, "\n")
	for i := 0; i < height-lines; i++ {
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

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
