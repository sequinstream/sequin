package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"

	"github.com/atotto/clipboard"
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
		updatedMessage, err := api.GetStreamMessage(ctx, m.streamName, m.detailMessage.Message.Key)
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
	// Create a map of existing messages for quick lookup
	existingMessages := make(map[string]*models.Message)
	for i := range m.messages {
		existingMessages[m.messages[i].Key] = &m.messages[i]
	}

	// Process upserted messages
	for _, upsertedMsg := range messages {
		if existingMsg, exists := existingMessages[upsertedMsg.Key]; exists {
			// Replace existing message if the upserted one is newer
			if upsertedMsg.UpdatedAt.After(existingMsg.UpdatedAt) {
				*existingMsg = upsertedMsg
			}
		} else {
			// Append new message
			m.messages = append(m.messages, upsertedMsg)
		}

		// Check if the upserted message is the selected message
		if m.detailMessage != nil && m.detailMessage.Message.Key == upsertedMsg.Key {
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
			if msg.Key == m.detailMessage.Message.Key {
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

func (m *MessageState) SetStreamName(streamName string) {
	m.streamName = streamName
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

	columnWidths := m.calculateColumnWidths(width)
	output += m.renderTableHeader(columnWidths, width)

	messagesHeight := height - lipgloss.Height(output)
	output += m.renderTableRows(columnWidths, messagesHeight)

	// Update visibleMessageCount
	m.visibleMessageCount = min(len(m.messages), messagesHeight)

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
		Background(lipgloss.Color(colorLightGray)).
		Width(totalWidth)

	tableHeader := fmt.Sprintf("%-*s %-*s %-*s %-*s",
		widths.seq, "SEQ",
		widths.key, "KEY",
		widths.created, "CREATED",
		widths.data, "DATA")

	return tableHeaderStyle.Render(tableHeader) + "\n"
}

func (m *MessageState) renderTableRows(widths TableColumnWidths, height int) string {
	var output string
	totalWidth := widths.seq + widths.key + widths.created + widths.data + 4 // 4 spaces between columns
	for i, msg := range m.messages {
		if i >= height {
			break
		}
		line := formatMessageLine(msg, widths)
		style := lipgloss.NewStyle().Width(totalWidth)
		if i == m.listCursor {
			style = style.
				Background(lipgloss.Color(colorPurple)).
				Foreground(lipgloss.Color(colorWhite))
		}
		output += style.Render(line) + "\n"
	}
	return output
}

func formatMessageLine(msg models.Message, widths TableColumnWidths) string {
	seq := fmt.Sprintf("%d", msg.Seq)
	key := truncateString(msg.Key, widths.key)
	created := msg.CreatedAt.Format(dateFormat)
	data := truncateString(msg.Data, widths.data)

	return fmt.Sprintf("%-*s %-*s %-*s %s",
		widths.seq, seq,
		widths.key, key,
		widths.created, created,
		data)
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

func (m *MessageState) detailView(width, _ int) string {
	if m.detailMessage == nil {
		return "No message selected or no messages available"
	}

	msg := m.detailMessage.Message
	output := lipgloss.NewStyle().Bold(true).Render("Message details") + "\n\n"

	// Define fields and their values
	fields := []struct {
		name  string
		value string
	}{
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

	slugWidth := 20
	deliverCountWidth := 15
	stateWidth := 20
	patternWidth := width - slugWidth - deliverCountWidth - stateWidth - 6

	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray)).
		Width(width)

	header := fmt.Sprintf("%-*s %-*s %-*s %-*s",
		slugWidth, "CONSUMER NAME",
		patternWidth, "CONSUMER FILTER PATTERN",
		stateWidth, "MESSAGE STATE",
		deliverCountWidth, "DELIVERED COUNT")

	output := tableHeaderStyle.Render(header) + "\n"

	for i, info := range consumerInfos {
		deliverCount := "-"
		if info.DeliverCount != nil {
			deliverCount = fmt.Sprintf("%d", *info.DeliverCount)
		}

		line := fmt.Sprintf("%-*s %-*s %-*s %-*s",
			slugWidth, truncateString(info.ConsumerName, slugWidth),
			patternWidth, truncateString(info.ConsumerFilterKeyPattern, patternWidth),
			stateWidth, info.State,
			deliverCountWidth, deliverCount)

		style := lipgloss.NewStyle().Width(width)
		if i == selectedIndex {
			style = style.
				Background(lipgloss.Color(colorPurple)).
				Foreground(lipgloss.Color(colorWhite))
		}

		output += style.Render(line) + "\n"
	}

	return output
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

	consumer_detail, err := api.FetchMessageWithConsumerInfos(ctx, m.streamName, m.detailMessage.Message.Key)
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
		textToCopy = fmt.Sprintf("%d", msg.Seq)
	case 1:
		textToCopy = msg.Key
	case 2:
		textToCopy = msg.CreatedAt.Format(dateFormat)
	case 3:
		textToCopy = msg.UpdatedAt.Format(dateFormat)
	case 4:
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
