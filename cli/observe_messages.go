package cli

import (
	"fmt"
	"strings"
	"time"

	"sequin-cli/api"
	"sequin-cli/context"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Message struct {
	messages    []api.Message
	config      *Config
	cursor      int
	showDetail  bool
	filter      string
	filterInput textinput.Model
	filterMode  bool
	err         error
	errorMsg    string // New field to store formatted error message
}

func NewMessage(config *Config) *Message {
	ti := textinput.New()
	ti.Placeholder = "Filter"
	ti.CharLimit = 100
	ti.Width = 30

	return &Message{
		config:      config,
		cursor:      0,
		showDetail:  false,
		filter:      "",
		filterInput: ti,
		filterMode:  false,
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
		m.errorMsg = fmt.Sprintf("Error fetching messages: %v", err)
		return nil
	}

	m.messages = messages
	m.errorMsg = "" // Clear any previous error message
	return nil
}

func (m *Message) View(width, height int) string {
	if m.err != nil {
		return fmt.Sprintf("Error: %v\n\nPress q to quit", m.err)
	}

	if m.showDetail {
		return m.detailView(width, height)
	}
	return m.listView(width, height)
}

func (m *Message) listView(width, height int) string {
	seqWidth := m.calculateSeqWidth()
	keyWidth := m.calculateKeyWidth(width)
	createdWidth := 22
	dataWidth := max(10, width-seqWidth-keyWidth-createdWidth-3)

	// Create the table header style
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("0")). // Black text
		Background(lipgloss.Color("2")). // Green background
		Width(width)

	// Add the "MESSAGES" title
	output := lipgloss.NewStyle().Bold(true).Render("MESSAGES") + "\n"

	// Add the filter input or filter display
	if m.filterMode {
		output += fmt.Sprintf("Filter (f): %s\n", strings.TrimPrefix(m.filterInput.View(), "> "))
	} else {
		output += fmt.Sprintf("Filter (f): %s\n", m.filter)
	}

	// Display error message if present
	if m.errorMsg != "" {
		errorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9")) // Red text
		output += errorStyle.Render(m.errorMsg) + "\n"
	} else {
		output += "\n"
	}

	// Format the table header
	tableHeader := fmt.Sprintf("%-*s %-*s %-*s %-*s",
		seqWidth, "SEQ",
		keyWidth, "KEY",
		createdWidth, "CREATED",
		dataWidth, "DATA")

	output += tableHeaderStyle.Render(tableHeader) + "\n"

	for i, msg := range m.messages {
		line := formatMessageLine(msg, seqWidth, keyWidth, createdWidth, dataWidth)
		style := lipgloss.NewStyle()
		if i == m.cursor {
			style = style.
				Background(lipgloss.Color("117")). // Light blue background
				Foreground(lipgloss.Color("0"))    // Black text
		}
		output += style.Render(line) + "\n"
	}

	return output
}

func (m *Message) calculateSeqWidth() int {
	maxSeqWidth := 3 // Minimum width for "Seq" header
	for _, msg := range m.messages {
		seqWidth := len(fmt.Sprintf("%d", msg.Seq))
		if seqWidth > maxSeqWidth {
			maxSeqWidth = seqWidth
		}
	}
	return maxSeqWidth
}

func (m *Message) calculateKeyWidth(totalWidth int) int {
	maxKeyWidth := 3 // Minimum width for "Key" header
	for _, msg := range m.messages {
		keyWidth := len(msg.Key)
		if keyWidth > maxKeyWidth {
			maxKeyWidth = keyWidth
		}
	}
	return min(min(maxKeyWidth, totalWidth/2), 255)
}

func formatMessageLine(msg api.Message, seqWidth, keyWidth, createdWidth, dataWidth int) string {
	seq := fmt.Sprintf("%d", msg.Seq)
	key := truncateString(msg.Key, keyWidth)
	created := msg.CreatedAt.Format(time.RFC3339)
	data := truncateString(msg.Data, dataWidth)

	return fmt.Sprintf("%-*s %-*s %-*s %-*s",
		seqWidth, seq,
		keyWidth, key,
		createdWidth, created,
		dataWidth, data)
}

func (m *Message) detailView(width, height int) string {
	msg := m.messages[m.cursor]
	output := lipgloss.NewStyle().Bold(true).Render("MESSAGE DETAIL")
	output += "\n\n"
	output += fmt.Sprintf("Seq:     %d\n", msg.Seq)
	output += fmt.Sprintf("Key:     %s\n", msg.Key)
	output += fmt.Sprintf("Created: %s\n", msg.CreatedAt.Format(time.RFC3339))

	output += formatDetailData(msg.Data)

	return output
}

func formatDetailData(data string) string {
	return fmt.Sprintf("Data:\n%s\n", data)
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

func (m *Message) DisableDetailView() {
	m.showDetail = false
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

func (m *Message) HandleFilterKey() {
	if !m.showDetail {
		m.filterMode = true
		m.filterInput.Focus()
	}
}

func (m *Message) HandleFilterModeKeyPress(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc", "enter", "ctrl+c":
		m.filterMode = false
		m.filterInput.Blur()
		m.filter = m.filterInput.Value()
		return m.ApplyFilter
	default:
		var cmd tea.Cmd
		m.filterInput, cmd = m.filterInput.Update(msg)
		return cmd
	}
}

func (m *Message) ApplyFilter() tea.Msg {
	filter := m.filter
	if filter == "" {
		filter = ">"
	}
	err := m.FetchMessages(calculateLimit(), filter)
	if err != nil {
		m.err = err
		m.errorMsg = fmt.Sprintf("Error: %v", err)
		return err
	}
	m.err = nil // Clear any previous error
	return nil
}
