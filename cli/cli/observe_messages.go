package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type MessageState struct {
	messages        []api.Message
	config          *Config
	cursor          int
	selectedMessage *api.Message
	showDetail      bool
	filter          string
	filterInput     textinput.Model
	filterMode      bool
	err             error
	errorMsg        string
	streamName      string // Changed from streamID to streamName
}

func NewMessageState(config *Config) *MessageState {
	ti := textinput.New()
	ti.Placeholder = "Filter"
	ti.CharLimit = 100
	ti.Width = 30

	return &MessageState{
		config:      config,
		cursor:      0,
		showDetail:  false,
		filter:      "",
		filterInput: ti,
		filterMode:  false,
		streamName:  "", // Changed from streamID to streamName
	}
}

func (m *MessageState) FetchMessages(limit int, filter string) error {
	if m.streamName == "" { // Changed from streamID to streamName
		return nil // No error, but we won't fetch messages
	}

	ctx, err := context.LoadContext(m.config.ContextName)
	if err != nil {
		return err
	}

	m.filter = filter
	messages, err := api.ListStreamMessages(ctx, m.streamName, limit, "seq_desc", filter) // Changed from streamID to streamName
	if err != nil {
		m.errorMsg = fmt.Sprintf("Error fetching messages: %v", err)
		return nil
	}

	m.messages = messages
	m.errorMsg = "" // Clear any previous error message

	// Refresh selectedMessage if it exists
	if m.selectedMessage != nil {
		updatedMessage, err := api.GetStreamMessage(ctx, m.streamName, m.selectedMessage.Key) // Changed from streamID to streamName
		if err != nil {
			m.errorMsg = fmt.Sprintf("Error refreshing selected message: %v", err)
		} else {
			m.selectedMessage = &updatedMessage
		}
	}

	return nil
}

func (m *MessageState) View(width, height int) string {
	if m.streamName == "" { // Changed from streamID to streamName
		return "\nPlease select a stream to view messages"
	}

	if m.err != nil {
		return fmt.Sprintf("Error: %v\n\nPress q to quit", m.err)
	}

	if m.showDetail {
		return m.detailView(width, height)
	}
	return m.listView(width, height)
}

func (m *MessageState) SetStreamName(streamName string) { // Changed from SetStreamID to SetStreamName
	m.streamName = streamName // Changed from streamID to streamName
	m.messages = nil
	m.cursor = 0
	m.selectedMessage = nil
	m.showDetail = false
	m.filter = ""
	m.filterInput.SetValue("")
	m.filterMode = false
	m.err = nil
	m.errorMsg = ""
}

func (m *MessageState) listView(width, height int) string {
	// Check if there are no messages
	if len(m.messages) == 0 {
		message := "No messages available\n\nTry adjusting your filter or adding messages to the stream:"
		codeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("12"))                                              // Blue color for code
		codePart := codeStyle.Render(fmt.Sprintf("sequin stream send %s message.key 'message payload'", m.streamName)) // Changed from streamID to streamName
		message += "\n\n" + codePart

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

func (m *MessageState) calculateSeqWidth() int {
	maxSeqWidth := 3 // Minimum width for "Seq" header
	for _, msg := range m.messages {
		seqWidth := len(fmt.Sprintf("%d", msg.Seq))
		if seqWidth > maxSeqWidth {
			maxSeqWidth = seqWidth
		}
	}
	return maxSeqWidth
}

func (m *MessageState) calculateKeyWidth(totalWidth int) int {
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

func (m *MessageState) detailView(_, _ int) string {
	if m.selectedMessage == nil {
		return "No message selected"
	}

	msg := *m.selectedMessage
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

func (m *MessageState) ToggleDetail() {
	m.showDetail = !m.showDetail
	if m.showDetail {
		m.selectedMessage = &m.messages[m.cursor]
	} else {
		m.updateCursorAfterDetailView()
	}
}

func (m *MessageState) updateCursorAfterDetailView() {
	if m.selectedMessage == nil {
		m.cursor = 0
		return
	}
	for i, msg := range m.messages {
		if msg.Seq == m.selectedMessage.Seq {
			m.cursor = i
			return
		}
	}
	m.cursor = 0
}

func (m *MessageState) MoveCursor(direction int) {
	m.cursor += direction
	m.cursor = clampValue(m.cursor, 0, len(m.messages)-1)
}

func (m *MessageState) IsDetailView() bool {
	return m.showDetail
}

func (m *MessageState) DisableDetailView() {
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

func (m *MessageState) HandleFilterKey() {
	if !m.showDetail {
		m.filterMode = true
		m.filterInput.Focus()
	}
}

func (m *MessageState) HandleFilterModeKeyPress(msg tea.KeyMsg) tea.Cmd {
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

func (m *MessageState) ApplyFilter() tea.Msg {
	if m.streamName == "" { // Changed from streamID to streamName
		return nil
	}

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
