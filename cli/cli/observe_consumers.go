package cli

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/cli/table"
	sequinContext "github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"

	"github.com/atotto/clipboard"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type DetailSection int

const (
	DetailSectionConsumer DetailSection = iota
	DetailSectionPending
	DetailSectionUpcoming
)

type ConsumerState struct {
	consumers                   []models.Consumer
	config                      *Config
	selectedConsumerID          string
	showDetail                  bool
	pendingMessages             []models.ConsumerMessage
	upcomingMessages            []models.ConsumerMessage
	streamName                  string
	ctx                         *sequinContext.Context
	isLoading                   bool
	detailSection               DetailSection
	detailCursor                int
	detailMaxCursor             int
	copiedNotification          string
	notificationTimer           *time.Timer
	observeChannel              *api.ObserveChannel
	messageLimit                int
	visiblePendingMessageCount  int
	visibleUpcomingMessageCount int
}

func NewConsumerState(config *Config, ctx *sequinContext.Context) *ConsumerState {
	return &ConsumerState{
		config:             config,
		ctx:                ctx,
		detailSection:      DetailSectionConsumer,
		detailCursor:       0,
		detailMaxCursor:    0,
		copiedNotification: "",
		notificationTimer:  nil,
		observeChannel:     nil,
	}
}

func (c *ConsumerState) FetchConsumers(limit int) error {
	if c.ctx == nil {
		return fmt.Errorf("context is not set")
	}

	if c.streamName == "" {
		return nil // No error, but we won't fetch consumers
	}

	consumers, err := api.FetchConsumers(c.ctx, c.streamName)
	if err != nil {
		return err
	}

	c.consumers = limitConsumers(consumers, limit)
	c.updateDetailView()

	// Select the first consumer if none is selected
	if c.selectedConsumerID == "" && len(c.consumers) > 0 {
		c.setSelectedConsumer(c.consumers[0].ID)
	}

	return nil
}

func limitConsumers(consumers []models.Consumer, limit int) []models.Consumer {
	if len(consumers) > limit {
		return consumers[:limit]
	}
	return consumers
}

func (c *ConsumerState) updateDetailView() {
	if len(c.consumers) == 0 {
		c.setSelectedConsumer("")
		c.DisableDetailView()
	} else if c.selectedConsumerID == "" {
		c.DisableDetailView()
	}
}

func (c *ConsumerState) setSelectedConsumer(id string) {
	if c.selectedConsumerID != id {
		c.selectedConsumerID = id
		c.resetMessages()
		c.resetDetailCursor()
		c.isLoading = true
	}
}

func (c *ConsumerState) updateMessages(pendingMessages, upcomingMessages []models.ConsumerMessage) {
	c.pendingMessages = pendingMessages
	c.upcomingMessages = upcomingMessages
	c.isLoading = false
}

func (c *ConsumerState) WillAppear(limit int) {
	c.messageLimit = limit
	c.FetchConsumers(limit)
}

func (c *ConsumerState) View(width, height int) string {
	if c.streamName == "" {
		return "\nPlease select a stream to view consumers"
	}

	if c.showDetail {
		return c.detailView(width, height)
	}
	return c.listView(width, height)
}

func (c *ConsumerState) listView(width, height int) string {
	if len(c.consumers) == 0 {
		message := "No consumers found\n\nTry creating a consumer for this stream:"
		codeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("12")) // Blue color for code
		codePart := codeStyle.Render(fmt.Sprintf("sequin consumer add %s", c.streamName))
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

	showFullTable := width >= 100

	columns := []table.Column{
		{Name: "NAME", MinWidth: 10, ValueFunc: func(row interface{}) string {
			return row.(models.Consumer).Name
		}},
		{Name: "FILTER PATTERN", MinWidth: 10, ValueFunc: func(row interface{}) string {
			return row.(models.Consumer).FilterKeyPattern
		}},
	}

	if showFullTable {
		columns = append(columns,
			table.Column{Name: "MAX ACK PENDING", MinWidth: 15, ValueFunc: func(row interface{}) string {
				return fmt.Sprintf("%d", row.(models.Consumer).MaxAckPending)
			}},
			table.Column{Name: "MAX DELIVER", MinWidth: 11, ValueFunc: func(row interface{}) string {
				return fmt.Sprintf("%d", row.(models.Consumer).MaxDeliver)
			}},
			table.Column{Name: "CREATED AT", MinWidth: 20, ValueFunc: func(row interface{}) string {
				return row.(models.Consumer).CreatedAt.Format(time.RFC3339)
			}},
		)
	}

	consumerTable := table.NewTable(
		columns,
		consumerInterfaceSlice(c.consumers),
		width,
	)

	consumerTable.SetActionColumn("SHOW DETAILS", func(row interface{}) string {
		return "Press enter"
	})

	consumerTable.HeaderStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray))
	consumerTable.RowStyle = lipgloss.NewStyle()
	consumerTable.SelectedStyle = lipgloss.NewStyle().
		Background(lipgloss.Color(colorPurple)).
		Foreground(lipgloss.Color(colorWhite))
	consumerTable.ActionOnSelect = true

	// Set the selected index based on the selectedConsumerID
	for i, consumer := range c.consumers {
		if consumer.ID == c.selectedConsumerID {
			consumerTable.SelectedIndex = i
			break
		}
	}

	output := lipgloss.NewStyle().Bold(true).Render("Consumers") + "\n\n"
	output += consumerTable.Render()

	return output
}

// Helper function to convert []models.Consumer to []interface{}
func consumerInterfaceSlice(consumers []models.Consumer) []interface{} {
	result := make([]interface{}, len(consumers))
	for i, v := range consumers {
		result[i] = v
	}
	return result
}

func (c *ConsumerState) detailView(width, height int) string {
	if len(c.consumers) == 0 || c.selectedConsumerID == "" {
		return "No consumer selected"
	}

	consumer := c.getSelectedConsumer()
	if consumer == nil {
		return "Selected consumer not found"
	}

	output := formatConsumerDetail(*consumer, c.detailSection == DetailSectionConsumer, c.detailCursor, c.copiedNotification, width)

	consumerDetailHeight := lipgloss.Height(formatConsumerDetail(*consumer, c.detailSection == DetailSectionConsumer, c.detailCursor, c.copiedNotification, width))

	remainingHeight := height - consumerDetailHeight

	pendingHeaderHeight := lipgloss.Height(formatMessageSection("Ack pending messages", nil, width, true, false, false, 0)) - 1
	upcomingHeaderHeight := lipgloss.Height(formatMessageSection("Available messages", nil, width, false, false, false, 0)) - 1

	messageTableHeight := remainingHeight - pendingHeaderHeight - upcomingHeaderHeight

	pendingCount := len(c.pendingMessages)
	upcomingCount := len(c.upcomingMessages)
	totalCount := pendingCount + upcomingCount

	var pendingLimit, upcomingLimit int

	if totalCount > messageTableHeight {
		// If we have more messages than we can display, try to show an equal number of each
		pendingLimit = messageTableHeight / 2
		upcomingLimit = messageTableHeight - pendingLimit
	} else {
		// If we have fewer messages than we can display, show all of them
		pendingLimit = pendingCount
		upcomingLimit = upcomingCount
	}

	// Adjust limits if one type has fewer messages than its limit
	if pendingCount < pendingLimit {
		upcomingLimit += pendingLimit - pendingCount
		pendingLimit = pendingCount
	} else if upcomingCount < upcomingLimit {
		pendingLimit += upcomingLimit - upcomingCount
		upcomingLimit = upcomingCount
	}

	c.visiblePendingMessageCount = min(pendingLimit, pendingCount)
	c.visibleUpcomingMessageCount = min(upcomingLimit, upcomingCount)
	c.detailMaxCursor = 5 + c.visiblePendingMessageCount + c.visibleUpcomingMessageCount

	if pendingCount > 0 {
		output += formatMessageSection("Ack pending messages", c.pendingMessages[:c.visiblePendingMessageCount], width, true, c.isLoading, c.detailSection == DetailSectionPending, c.detailCursor)
	} else {
		output += formatMessageSection("Ack pending messages", nil, width, true, c.isLoading, c.detailSection == DetailSectionPending, c.detailCursor)
	}

	if upcomingCount > 0 {
		output += formatMessageSection("Available messages", c.upcomingMessages[:c.visibleUpcomingMessageCount], width, false, c.isLoading, c.detailSection == DetailSectionUpcoming, c.detailCursor)
	} else {
		output += formatMessageSection("Available messages", nil, width, false, c.isLoading, c.detailSection == DetailSectionUpcoming, c.detailCursor)
	}

	return output
}

func (c *ConsumerState) getSelectedConsumer() *models.Consumer {
	for _, consumer := range c.consumers {
		if consumer.ID == c.selectedConsumerID {
			return &consumer
		}
	}
	return nil
}

func formatConsumerDetail(consumer models.Consumer, isSelected bool, cursor int, copiedNotification string, width int) string {
	output := lipgloss.NewStyle().Bold(true).Render("Consumer details")
	output += "\n\n"

	fields := []struct {
		name  string
		value string
	}{
		{"ID", consumer.ID},
		{"Name", consumer.Name},
		{"Filter", consumer.FilterKeyPattern},
		{"Max Ack Pending", fmt.Sprintf("%d", consumer.MaxAckPending)},
		{"Max Deliver", fmt.Sprintf("%d", consumer.MaxDeliver)},
		{"Created At", consumer.CreatedAt.Format(time.RFC3339)},
	}

	copyTextStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(colorWhite)).
		Align(lipgloss.Right).
		PaddingRight(2)

	for i, field := range fields {
		style := lipgloss.NewStyle().Width(width)
		line := fmt.Sprintf("%-17s %s", field.name+":", field.value)
		if isSelected && i == cursor {
			style = style.Background(lipgloss.Color(colorPurple)).Foreground(lipgloss.Color(colorWhite))
			copyText := getCopyText(copyTextStyle, copiedNotification)
			remainingWidth := width - lipgloss.Width(line) - lipgloss.Width(copyText)
			if remainingWidth > 0 {
				line += strings.Repeat(" ", remainingWidth) + copyText
			} else {
				// If there's not enough space, truncate the line
				line = lipgloss.NewStyle().Width(width-lipgloss.Width(copyText)).Render(line) + copyText
			}
		}
		output += style.Render(line) + "\n"
	}

	return output
}

func getCopyText(style lipgloss.Style, copiedNotification string) string {
	text := "Press enter to copy"
	if copiedNotification != "" {
		text = copiedNotification
	}
	return style.Render(text)
}

func formatMessageSection(title string, messages []models.ConsumerMessage, width int, isPending, isLoading, isSelected bool, cursor int) string {
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Width(width)
	output := "\n" + headerStyle.Render(title) + "\n"
	if isLoading {
		return output + loadingSpinner()
	}
	return output + formatMessageList(messages, width, isPending, isSelected, cursor)
}

func formatMessageList(messages []models.ConsumerMessage, width int, isPending, isSelected bool, cursor int) string {
	if len(messages) == 0 {
		return "No messages available\n"
	}

	columns := []table.Column{
		{Name: "SEQ", MinWidth: 4, ValueFunc: func(row interface{}) string {
			return fmt.Sprintf("%d", row.(models.ConsumerMessage).MessageSeq)
		}},
		{Name: "KEY", MinWidth: 10, ValueFunc: func(row interface{}) string {
			return row.(models.ConsumerMessage).MessageKey
		}},
		{Name: "DELIVER", MinWidth: 7, ValueFunc: func(row interface{}) string {
			return fmt.Sprintf("%d", row.(models.ConsumerMessage).DeliverCount)
		}},
	}

	lastColumnName := "Not Visible Until"
	if !isPending {
		lastColumnName = "CREATED AT"
	}
	columns = append(columns, table.Column{
		Name:     lastColumnName,
		MinWidth: 20,
		ValueFunc: func(row interface{}) string {
			msg := row.(models.ConsumerMessage)
			if isPending {
				return msg.FormatNotVisibleUntil()
			}
			return msg.Message.CreatedAt.Format(time.RFC3339)
		},
	})

	messageTable := table.NewTable(
		columns,
		consumerMessageInterfaceSlice(messages),
		width,
	)

	messageTable.HeaderStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray))
	messageTable.RowStyle = lipgloss.NewStyle()
	messageTable.SelectedStyle = lipgloss.NewStyle().
		Background(lipgloss.Color(colorPurple)).
		Foreground(lipgloss.Color(colorWhite))

	if isSelected {
		messageTable.SelectedIndex = cursor
	} else {
		messageTable.SelectedIndex = -1 // No selection
	}

	return messageTable.Render()
}

// Helper function to convert []models.ConsumerMessage to []interface{}
func consumerMessageInterfaceSlice(messages []models.ConsumerMessage) []interface{} {
	result := make([]interface{}, len(messages))
	for i, v := range messages {
		result[i] = v
	}
	return result
}

func (c *ConsumerState) ToggleDetail() {
	if c.showDetail {
		c.DisableDetailView()
	} else if c.selectedConsumerID != "" {
		c.EnableDetailView()
	}
}

func (c *ConsumerState) MoveCursor(direction int) {
	if len(c.consumers) == 0 {
		return
	}

	currentIndex := c.getCurrentIndex()
	newIndex := clamp(currentIndex+direction, 0, len(c.consumers)-1)

	if currentIndex != newIndex {
		c.setSelectedConsumer(c.consumers[newIndex].ID)
	}
}

func (c *ConsumerState) MoveDetailCursor(direction int) {
	c.clearNotification() // Clear notification when cursor moves

	switch c.detailSection {
	case DetailSectionConsumer:
		c.detailCursor += direction
		if c.detailCursor < 0 {
			c.detailCursor = 0
		} else if c.detailCursor > 5 {
			if c.visiblePendingMessageCount > 0 {
				c.detailSection = DetailSectionPending
				c.detailCursor = 0
			} else if c.visibleUpcomingMessageCount > 0 {
				c.detailSection = DetailSectionUpcoming
				c.detailCursor = 0
			} else {
				c.detailCursor = 5 // Stay in consumer section if both message sections are empty
			}
		}
	case DetailSectionPending:
		c.detailCursor += direction
		if c.detailCursor < 0 {
			c.detailSection = DetailSectionConsumer
			c.detailCursor = 5
		} else if c.detailCursor >= c.visiblePendingMessageCount {
			if c.visibleUpcomingMessageCount > 0 {
				c.detailSection = DetailSectionUpcoming
				c.detailCursor = 0
			} else {
				c.detailCursor = c.visiblePendingMessageCount - 1 // Stay at last pending message if upcoming is empty
			}
		}
	case DetailSectionUpcoming:
		c.detailCursor += direction
		if c.detailCursor < 0 {
			if c.visiblePendingMessageCount > 0 {
				c.detailSection = DetailSectionPending
				c.detailCursor = c.visiblePendingMessageCount - 1
			} else {
				c.detailSection = DetailSectionConsumer
				c.detailCursor = 5
			}
		} else if c.detailCursor >= c.visibleUpcomingMessageCount {
			c.detailCursor = c.visibleUpcomingMessageCount - 1
		}
	}
}

func (c *ConsumerState) getCurrentIndex() int {
	for i, consumer := range c.consumers {
		if consumer.ID == c.selectedConsumerID {
			return i
		}
	}
	return 0
}

func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func (c *ConsumerState) resetMessages() {
	c.pendingMessages = nil
	c.upcomingMessages = nil
	c.isLoading = true
}

func (c *ConsumerState) resetDetailCursor() {
	c.detailCursor = 0
	c.detailSection = DetailSectionConsumer
}

func (c *ConsumerState) SetStream(stream models.Stream) {
	c.streamName = stream.Name
	c.consumers = nil
	c.selectedConsumerID = ""
	c.DisableDetailView()
	c.pendingMessages = nil
	c.upcomingMessages = nil
	c.isLoading = false
	c.resetDetailCursor()
}

func (c *ConsumerState) EnableDetailView() {
	c.showDetail = true
	c.resetDetailCursor()

	if c.observeChannel != nil {
		log.Printf("Listening to consumer: %s\n\tmessageLimit: %d", c.selectedConsumerID, c.messageLimit)
		c.observeChannel.ListenConsumer(c.selectedConsumerID, c.messageLimit)
		c.observeChannel.SetConsumerMessagesCallback(c.updateMessages)
	}
}

func (c *ConsumerState) DisableDetailView() {
	c.showDetail = false
	c.resetDetailCursor()

	if c.observeChannel != nil {
		c.observeChannel.ClearListeningConsumer()
	}
}

func (c *ConsumerState) HandleDetailViewKeyPress(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "up", "k":
		c.MoveDetailCursor(-1)
		c.clearNotification()
	case "down", "j":
		c.MoveDetailCursor(1)
		c.clearNotification()
	case "enter":
		return c.handleDetailViewEnter()
	}
	return nil
}

func (c *ConsumerState) handleDetailViewEnter() tea.Cmd {
	consumer := c.getSelectedConsumer()
	if consumer == nil {
		return nil
	}

	var textToCopy string

	switch c.detailCursor {
	case 0:
		textToCopy = consumer.ID
	case 1:
		textToCopy = consumer.Name
	case 2:
		textToCopy = consumer.FilterKeyPattern
	case 3:
		textToCopy = fmt.Sprintf("%d", consumer.MaxAckPending)
	case 4:
		textToCopy = fmt.Sprintf("%d", consumer.MaxDeliver)
	case 5:
		textToCopy = consumer.CreatedAt.Format(time.RFC3339)
	default:
		return nil
	}

	return tea.Sequence(
		c.copyToClipboard(textToCopy),
		c.showCopiedNotification(),
	)
}

func (c *ConsumerState) copyToClipboard(text string) tea.Cmd {
	return func() tea.Msg {
		err := clipboard.WriteAll(text)
		if err != nil {
			return fmt.Errorf("failed to copy to clipboard: %v", err)
		}
		return nil
	}
}

func (c *ConsumerState) showCopiedNotification() tea.Cmd {
	return func() tea.Msg {
		c.copiedNotification = "Copied to clipboard!"
		if c.notificationTimer != nil {
			c.notificationTimer.Stop()
		}
		c.notificationTimer = time.AfterFunc(3*time.Second, func() {
			c.copiedNotification = ""
			c.notificationTimer = nil
		})
		return nil
	}
}

func (c *ConsumerState) clearNotification() {
	c.copiedNotification = ""
	if c.notificationTimer != nil {
		c.notificationTimer.Stop()
		c.notificationTimer = nil
	}
}

func (c *ConsumerState) SetObserveChannel(oc *api.ObserveChannel) {
	c.observeChannel = oc
}
