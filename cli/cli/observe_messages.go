package cli

import (
	"fmt"
	"strings"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type TableColumnWidths struct {
	seq     int
	key     int
	created int
	data    int
}

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
	streamName      string
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
		streamName:  "",
	}
}

func (m *MessageState) FetchMessages(limit int, filter string) error {
	if m.streamName == "" {
		return nil
	}

	ctx, err := context.LoadContext(m.config.ContextName)
	if err != nil {
		return err
	}

	m.filter = filter
	messages, err := api.ListStreamMessages(ctx, m.streamName, limit, "seq_desc", filter)
	if err != nil {
		m.errorMsg = fmt.Sprintf("Error fetching messages: %v", err)
		return nil
	}

	m.messages = messages
	m.errorMsg = ""

	if m.selectedMessage != nil {
		updatedMessage, err := api.GetStreamMessage(ctx, m.streamName, m.selectedMessage.Key)
		if err != nil {
			m.errorMsg = fmt.Sprintf("Error refreshing selected message: %v", err)
		} else {
			m.selectedMessage = &updatedMessage
		}
	}

	return nil
}

func (m *MessageState) View(width, height int) string {
	if m.streamName == "" {
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

func (m *MessageState) SetStreamName(streamName string) {
	m.streamName = streamName
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
	output := lipgloss.NewStyle().Bold(true).Render("Select a message to view details") + "\n"

	if m.filterMode {
		output += fmt.Sprintf("Filter (f): %s\n", strings.TrimPrefix(m.filterInput.View(), "> "))
	} else {
		output += fmt.Sprintf("Filter (f): %s\n", m.filter)
	}

	if m.errorMsg != "" {
		errorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(colorRed))
		output += errorStyle.Render(m.errorMsg) + "\n"
	} else {
		output += "\n"
	}

	if len(m.messages) == 0 {
		message := "No messages available\n\nTry adjusting your filter or adding messages to the stream:"
		codeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("12"))
		codePart := codeStyle.Render(fmt.Sprintf("sequin stream send %s message.key 'message payload'", m.streamName))
		message += "\n\n" + codePart

		lines := strings.Split(message, "\n")
		verticalPadding := (height - len(lines) - 3) / 2

		output += strings.Repeat("\n", verticalPadding)

		for _, line := range lines {
			horizontalPadding := (width - lipgloss.Width(line)) / 2
			output += fmt.Sprintf("%s%s\n", strings.Repeat(" ", horizontalPadding), line)
		}

		return output
	}

	columnWidths := m.calculateColumnWidths(width)
	output += m.renderTableHeader(columnWidths, width)
	output += m.renderTableRows(columnWidths)

	return output
}

func (m *MessageState) calculateColumnWidths(width int) TableColumnWidths {
	seqWidth := m.calculateSeqWidth()
	keyWidth := m.calculateKeyWidth(width)
	createdWidth := 22
	dataWidth := max(10, width-seqWidth-keyWidth-createdWidth-4) // 4 spaces between columns

	// Distribute extra space evenly among columns
	extraSpace := width - (seqWidth + keyWidth + createdWidth + dataWidth + 4)
	if extraSpace > 0 {
		extraPerColumn := extraSpace / 4
		seqWidth += extraPerColumn
		keyWidth += extraPerColumn
		createdWidth += extraPerColumn
		dataWidth += extraSpace - (3 * extraPerColumn) // Add remaining space to data column
	}

	return TableColumnWidths{
		seq:     seqWidth,
		key:     keyWidth,
		created: createdWidth,
		data:    dataWidth,
	}
}

func (m *MessageState) renderTableHeader(widths TableColumnWidths, totalWidth int) string {
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorGreen)).
		Width(totalWidth)

	tableHeader := fmt.Sprintf("%-*s %-*s %-*s %-*s",
		widths.seq, "SEQ",
		widths.key, "KEY",
		widths.created, "CREATED",
		widths.data, "DATA")

	return tableHeaderStyle.Render(tableHeader) + "\n"
}

func (m *MessageState) renderTableRows(widths TableColumnWidths) string {
	var output string
	for i, msg := range m.messages {
		line := formatMessageLine(msg, widths)
		style := lipgloss.NewStyle()
		if i == m.cursor {
			style = style.
				Background(lipgloss.Color(colorLightBlue)).
				Foreground(lipgloss.Color(colorBlack))
		}
		output += style.Render(line) + "\n"
	}
	return output
}

func formatMessageLine(msg api.Message, widths TableColumnWidths) string {
	seq := fmt.Sprintf("%d", msg.Seq)
	key := truncateString(msg.Key, widths.key)
	created := msg.CreatedAt.Format(dateFormat)
	data := truncateString(msg.Data, widths.data)

	return fmt.Sprintf("%-*s %-*s %-*s %-*s",
		widths.seq, seq,
		widths.key, key,
		widths.created, created,
		widths.data, data)
}

func (m *MessageState) calculateSeqWidth() int {
	maxSeqWidth := 3
	for _, msg := range m.messages {
		seqWidth := len(fmt.Sprintf("%d", msg.Seq))
		if seqWidth > maxSeqWidth {
			maxSeqWidth = seqWidth
		}
	}
	return maxSeqWidth
}

func (m *MessageState) calculateKeyWidth(totalWidth int) int {
	maxKeyWidth := 3
	for _, msg := range m.messages {
		keyWidth := len(msg.Key)
		if keyWidth > maxKeyWidth {
			maxKeyWidth = keyWidth
		}
	}
	return min(min(maxKeyWidth, totalWidth/2), 255)
}

func (m *MessageState) detailView(_, _ int) string {
	if m.selectedMessage == nil {
		return "No message selected or no messages available"
	}

	msg := *m.selectedMessage
	output := lipgloss.NewStyle().Bold(true).Render("Message details")
	output += "\n\n"
	output += fmt.Sprintf("Seq:     %d\n", msg.Seq)
	output += fmt.Sprintf("Key:     %s\n", msg.Key)
	output += fmt.Sprintf("Created: %s\n", msg.CreatedAt.Format(dateFormat))

	output += formatDetailData(msg.Data)

	return output
}

func formatDetailData(data string) string {
	return fmt.Sprintf("Data:\n%s\n", data)
}

func (m *MessageState) ToggleDetail() {
	m.showDetail = !m.showDetail
	if m.showDetail {
		// Only set selectedMessage if there are messages
		if len(m.messages) > 0 {
			m.selectedMessage = &m.messages[m.cursor]
		} else {
			m.selectedMessage = nil
		}
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
	if m.streamName == "" {
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
	m.err = nil
	return nil
}
