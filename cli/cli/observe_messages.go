package cli

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/cli/table"
	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"

	"github.com/atotto/clipboard"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type MessageState struct {
	messages            []models.Message
	config              *Config
	listCursor          int
	detailMessage       *models.MessageWithConsumerInfos
	showDetail          bool
	filterPattern       string
	filterInput         textinput.Model
	filterMode          bool
	err                 error
	errorMsg            string
	streamName          string
	visibleMessageCount int
	detailCursor        int
	detailMaxCursor     int
	copiedNotification  string
	notificationTimer   *time.Timer
	streamID            string
	mu                  sync.Mutex
}

type MessageWithConsumerInfos struct {
	Message       models.MessageInfo    `json:"message"`
	ConsumerInfos []models.ConsumerInfo `json:"consumer_info"`
}

func NewMessageState(config *Config) *MessageState {
	ti := textinput.New()
	ti.Placeholder = "Filter"
	ti.CharLimit = 100
	ti.Width = 30

	return &MessageState{
		config:              config,
		listCursor:          0,
		showDetail:          false,
		filterPattern:       "",
		filterInput:         ti,
		filterMode:          false,
		streamName:          "",
		visibleMessageCount: 0,
		detailCursor:        0,
		detailMaxCursor:     0,
		copiedNotification:  "",
		notificationTimer:   nil,
	}
}

func (m *MessageState) FetchMessages(limit int, filterPattern string) error {
	if m.streamName == "" {
		return nil
	}

	ctx, err := context.LoadContext(m.config.ContextName)
	if err != nil {
		return err
	}

	m.filterPattern = filterPattern
	messages, err := api.ListStreamMessages(ctx, m.streamName, limit, "seq_desc", filterPattern)
	if err != nil {
		m.errorMsg = fmt.Sprintf("Error fetching messages: %v", err)
		return nil
	}

	m.messages = messages
	m.errorMsg = ""

	if m.detailMessage != nil {
		updatedMessage, err := api.GetStreamMessage(ctx, m.streamName, m.detailMessage.Message.ID)
		if err != nil {
			m.errorMsg = fmt.Sprintf("Error refreshing selected message: %v", err)
		} else {
			m.detailMessage.Message = updatedMessage
			m.fetchMessageWithConsumerInfos()
		}
	}

	return nil
}

func (m *MessageState) MessagesUpserted(messages []models.Message, limit int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	filteredMessages := m.filterMessages(messages)

	// Create a map of existing messages for quick lookup
	existingMessages := make(map[string]*models.Message)
	for i := range m.messages {
		existingMessages[m.messages[i].ID] = &m.messages[i]
	}

	// Process upserted messages
	for _, upsertedMsg := range filteredMessages {
		if existingMsg, exists := existingMessages[upsertedMsg.ID]; exists {
			// Replace existing message if the upserted one is newer
			if upsertedMsg.UpdatedAt.After(existingMsg.UpdatedAt) {
				*existingMsg = upsertedMsg
			}
		} else {
			// Append new message
			m.messages = append(m.messages, upsertedMsg)
		}

		// Check if the upserted message is the selected message
		if m.detailMessage != nil && m.detailMessage.Message.ID == upsertedMsg.ID {
			m.detailMessage.Message = upsertedMsg
			m.fetchMessageWithConsumerInfos()
		}
	}

	// Sort messages by seq desc
	sort.Slice(m.messages, func(i, j int) bool {
		return m.messages[i].Seq > m.messages[j].Seq
	})

	// Limit the final list
	if len(m.messages) > limit {
		m.messages = m.messages[:limit]
	}

	// Update detailMessage if it's no longer in the messages slice
	if m.detailMessage != nil {
		for _, msg := range m.messages {
			if msg.ID == m.detailMessage.Message.ID {
				m.detailMessage.Message = msg
				break
			}
		}
	}
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

func (m *MessageState) SetStream(stream models.Stream) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamName = stream.Name
	m.streamID = stream.ID
	m.messages = nil
	m.listCursor = 0
	m.detailMessage = nil
	m.showDetail = false
	m.filterPattern = ""
	m.filterInput.SetValue("")
	m.filterMode = false
	m.err = nil
	m.errorMsg = ""
	m.detailCursor = 0
	m.detailMaxCursor = 0
}

func (m *MessageState) listView(width, height int) string {
	output := lipgloss.NewStyle().Bold(true).Render("Select a message to view details") + "\n"
	if m.filterMode {
		output += fmt.Sprintf("Filter (f): %s\n", strings.TrimPrefix(m.filterInput.View(), "> "))
	} else {
		output += fmt.Sprintf("Filter (f): %s\n", m.filterPattern)
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

	// Calculate available height for the table
	tableHeight := height - lipgloss.Height(output)

	// Create a new table
	messageTable := table.NewTable(
		[]table.Column{
			{Name: "SEQ", MinWidth: 4, ValueFunc: func(row interface{}) string {
				return fmt.Sprintf("%d", row.(models.Message).Seq)
			}},
			{Name: "KEY", MinWidth: 10, ValueFunc: func(row interface{}) string {
				return row.(models.Message).Key
			}},
			{Name: "CREATED", MinWidth: 22, ValueFunc: func(row interface{}) string {
				return row.(models.Message).CreatedAt.Format(dateFormat)
			}},
			{Name: "DATA", MinWidth: 10, ValueFunc: func(row interface{}) string {
				data := row.(models.Message).Data
				return strings.ReplaceAll(data, "\n", " ")
			}},
		},
		messageInterfaceSlice(m.messages),
		width,
	)

	messageTable.SelectedIndex = m.listCursor
	messageTable.HeaderStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray))
	messageTable.RowStyle = lipgloss.NewStyle()
	messageTable.SelectedStyle = lipgloss.NewStyle().
		Background(lipgloss.Color(colorPurple)).
		Foreground(lipgloss.Color(colorWhite))

	messageTable.SetActionColumn("View Details", func(row interface{}) string {
		return "Press Enter"
	})
	messageTable.SelectedIndex = m.listCursor
	messageTable.ActionOnSelect = true

	// Render the table
	tableOutput := messageTable.Render()

	// Truncate the table output if it exceeds the available height
	tableLines := strings.Split(tableOutput, "\n")
	if tableHeight > 0 && len(tableLines) > tableHeight {
		tableLines = tableLines[:tableHeight]
		tableOutput = strings.Join(tableLines, "\n")
	}

	output += tableOutput

	// Update visibleMessageCount
	m.visibleMessageCount = max(0, min(len(m.messages), tableHeight-1)) // Ensure non-negative

	return output
}

// Helper function to convert []models.Message to []interface{}
func messageInterfaceSlice(messages []models.Message) []interface{} {
	result := make([]interface{}, len(messages))
	for i, v := range messages {
		result[i] = v
	}
	return result
}

func (m *MessageState) detailView(width, _ int) string {
	if m.detailMessage == nil {
		return "No message selected"
	}

	msg := m.detailMessage.Message
	output := lipgloss.NewStyle().Bold(true).Render("Message details") + "\n\n"

	// Define fields and their values
	fields := []struct {
		name  string
		value string
	}{
		{"ID", msg.ID},
		{"Seq", fmt.Sprintf("%d", msg.Seq)},
		{"Key", msg.Key},
		{"Created", msg.CreatedAt.Format(dateFormat)},
		{"Updated", msg.UpdatedAt.Format(dateFormat)},
	}

	// Render fields
	copyTextStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(colorWhite)).
		Align(lipgloss.Right).
		PaddingRight(2)

	for i, field := range fields {
		style := lipgloss.NewStyle().Width(width)
		line := fmt.Sprintf("%-15s %s", field.name+":", field.value)
		if i == m.detailCursor {
			style = style.
				Background(lipgloss.Color(colorPurple)).
				Foreground(lipgloss.Color(colorWhite))
			copyText := m.getCopyText(copyTextStyle, width-lipgloss.Width(line))
			line += copyText
		}
		output += style.Render(line) + "\n"
	}
	output += "\n"

	// Add the data value separately, allowing it to wrap
	dataStyle := lipgloss.NewStyle().Bold(true).Width(width)
	if m.detailCursor == len(fields) {
		dataStyle = dataStyle.
			Background(lipgloss.Color(colorPurple)).
			Foreground(lipgloss.Color(colorWhite))
		copyText := m.getCopyText(copyTextStyle, 0)
		output += dataStyle.Render(fmt.Sprintf("%-*s%s", width-lipgloss.Width(copyText), "Data", copyText))
	} else {
		output += dataStyle.Render("Data")
	}
	output += "\n"
	output += wrapText(msg.Data, width) + "\n\n"

	output += lipgloss.NewStyle().Bold(true).Width(width).Render("Consumers for message") + "\n\n"
	output += formatConsumerInfoTable(m.detailMessage.ConsumerInfos, width, m.detailCursor-len(fields)-1)

	m.detailMaxCursor = len(fields) + len(m.detailMessage.ConsumerInfos)

	return output
}

func wrapText(text string, width int) string {
	var wrapped []string
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		if len(line) <= width {
			wrapped = append(wrapped, line)
		} else {
			for len(line) > width {
				i := strings.LastIndex(line[:width], " ")
				if i == -1 {
					i = width
				}
				wrapped = append(wrapped, line[:i])
				line = line[i:]
				if len(line) > 0 && line[0] == ' ' {
					line = line[1:]
				}
			}
			if len(line) > 0 {
				wrapped = append(wrapped, line)
			}
		}
	}
	return strings.Join(wrapped, "\n")
}

func formatConsumerInfoTable(consumerInfos []models.ConsumerInfo, width int, selectedIndex int) string {
	if len(consumerInfos) == 0 {
		return lipgloss.NewStyle().Width(width).Render("No consumers for message\n")
	}

	consumerTable := table.NewTable(
		[]table.Column{
			{Name: "CONSUMER NAME", MinWidth: 20, ValueFunc: func(row interface{}) string {
				return row.(models.ConsumerInfo).ConsumerName
			}},
			{Name: "CONSUMER FILTER PATTERN", MinWidth: 20, ValueFunc: func(row interface{}) string {
				return row.(models.ConsumerInfo).ConsumerFilterKeyPattern
			}},
			{Name: "MESSAGE STATE", MinWidth: 20, ValueFunc: func(row interface{}) string {
				return row.(models.ConsumerInfo).State
			}},
			{Name: "DELIVERED COUNT", MinWidth: 15, ValueFunc: func(row interface{}) string {
				info := row.(models.ConsumerInfo)
				if info.DeliverCount != nil {
					return fmt.Sprintf("%d", *info.DeliverCount)
				}
				return "-"
			}},
		},
		consumerInfoInterfaceSlice(consumerInfos),
		width,
	)

	consumerTable.SelectedIndex = selectedIndex
	consumerTable.HeaderStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray))
	consumerTable.RowStyle = lipgloss.NewStyle()
	consumerTable.SelectedStyle = lipgloss.NewStyle().
		Background(lipgloss.Color(colorPurple)).
		Foreground(lipgloss.Color(colorWhite))

	return consumerTable.Render()
}

// Helper function to convert []models.ConsumerInfo to []interface{}
func consumerInfoInterfaceSlice(consumerInfos []models.ConsumerInfo) []interface{} {
	result := make([]interface{}, len(consumerInfos))
	for i, v := range consumerInfos {
		result[i] = v
	}
	return result
}

func (m *MessageState) ToggleDetail() {
	m.showDetail = !m.showDetail
	if m.showDetail {
		// Only set detailMessage if there are messages
		if len(m.messages) > m.listCursor {
			m.detailMessage = &models.MessageWithConsumerInfos{
				Message:       m.messages[m.listCursor],
				ConsumerInfos: nil,
			}
			m.fetchMessageWithConsumerInfos()
		} else {
			m.detailMessage = nil
		}
	} else {
		m.detailMessage = nil
		m.updateCursorAfterDetailView()
	}
}

func (m *MessageState) fetchMessageWithConsumerInfos() error {
	ctx, err := context.LoadContext(m.config.ContextName)
	if err != nil {
		return err
	}

	consumer_detail, err := api.FetchMessageWithConsumerInfos(ctx, m.streamName, m.detailMessage.Message.ID)
	if err != nil {
		return err
	}

	m.detailMessage = &models.MessageWithConsumerInfos{
		Message:       consumer_detail.Message,
		ConsumerInfos: consumer_detail.ConsumerInfos,
	}
	return nil
}

func (m *MessageState) updateCursorAfterDetailView() {
	if m.detailMessage == nil {
		m.listCursor = 0
		return
	}
	for i, msg := range m.messages {
		if msg.Seq == m.detailMessage.Message.Seq {
			m.listCursor = i
			return
		}
	}
	m.listCursor = 0
}

func (m *MessageState) MoveCursor(direction int, amount int) {
	m.listCursor += direction * amount
	m.listCursor = clampValue(m.listCursor, 0, m.visibleMessageCount-1)

	// Set detailMessage
	m.detailMessage = &models.MessageWithConsumerInfos{
		Message:       m.messages[m.listCursor],
		ConsumerInfos: nil,
	}
	m.fetchMessageWithConsumerInfos()
}

func (m *MessageState) PageMove(direction int, height int) {
	pageSize := height / 2 // Move by half the height
	m.listCursor += direction * pageSize
	m.listCursor = clampValue(m.listCursor, 0, m.visibleMessageCount-1)
}

func (m *MessageState) IsDetailView() bool {
	return m.showDetail
}

func (m *MessageState) DisableDetailView() {
	m.showDetail = false
	m.detailMessage = nil
}

func truncateString(s string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	if idx := strings.Index(s, "\n"); idx != -1 {
		s = s[:idx]
	}
	if maxLen <= 3 && len(s) > 3 {
		return strings.Repeat(".", maxLen)
	}
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
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
		m.filterPattern = m.filterInput.Value()
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

	filterPattern := m.filterPattern
	if filterPattern == "" {
		filterPattern = ">"
	}
	err := m.FetchMessages(calculateLimit(), filterPattern)
	if err != nil {
		m.err = err
		m.errorMsg = fmt.Sprintf("Error: %v", err)
		return err
	}
	m.err = nil
	return nil
}

func (m *MessageState) HandleDetailViewKeyPress(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "up", "k":
		m.detailCursor--
		if m.detailCursor < 0 {
			m.detailCursor = 0
		}
		m.clearNotification()
	case "down", "j":
		m.detailCursor++
		if m.detailCursor > m.detailMaxCursor {
			m.detailCursor = m.detailMaxCursor
		}
		m.clearNotification()
	case "enter":
		return m.handleDetailViewEnter()
	}
	return nil
}

func (m *MessageState) handleDetailViewEnter() tea.Cmd {
	msg := m.detailMessage.Message
	var textToCopy string

	switch m.detailCursor {
	case 0:
		textToCopy = msg.ID
	case 1:
		textToCopy = fmt.Sprintf("%d", msg.Seq)
	case 2:
		textToCopy = msg.Key
	case 3:
		textToCopy = msg.CreatedAt.Format(dateFormat)
	case 4:
		textToCopy = msg.UpdatedAt.Format(dateFormat)
	case 5:
		textToCopy = msg.Data
	default:
		return nil
	}

	return tea.Sequence(
		m.copyToClipboard(textToCopy),
		m.showCopiedNotification(),
	)
}

func (m *MessageState) copyToClipboard(text string) tea.Cmd {
	return func() tea.Msg {
		err := clipboard.WriteAll(text)
		if err != nil {
			return fmt.Errorf("failed to copy to clipboard: %v", err)
		}
		return nil
	}
}

func (m *MessageState) showCopiedNotification() tea.Cmd {
	return func() tea.Msg {
		m.copiedNotification = "Copied to clipboard!"
		if m.notificationTimer != nil {
			m.notificationTimer.Stop()
		}
		m.notificationTimer = time.AfterFunc(3*time.Second, func() {
			m.copiedNotification = ""
			m.notificationTimer = nil
		})
		return nil
	}
}

func (m *MessageState) getCopyText(style lipgloss.Style, remainingWidth int) string {
	text := "Press enter to copy"
	if m.copiedNotification != "" {
		text = m.copiedNotification
	}
	return style.Width(remainingWidth).Render(text)
}

func (m *MessageState) clearNotification() {
	m.copiedNotification = ""
	if m.notificationTimer != nil {
		m.notificationTimer.Stop()
		m.notificationTimer = nil
	}
}

func (m *MessageState) filterMessages(messages []models.Message) []models.Message {
	if m.streamID == "" {
		return nil
	}

	filtered := make([]models.Message, 0)
	for _, msg := range messages {
		if msg.StreamID == m.streamID {
			if m.filterPattern != "" {
				if DoesKeyMatch(m.filterPattern, msg.Key) {
					filtered = append(filtered, msg)
				}
			} else {
				filtered = append(filtered, msg)
			}
		}
	}
	return filtered
}

// Add this helper function if it doesn't exist
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
