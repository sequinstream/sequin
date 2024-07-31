package cli

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/sequinstream/sequin/cli/api"
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
	c.FetchConsumers(limit)
}

func (c *ConsumerState) View(width, height int) string {
	if c.streamName == "" {
		return "\nPlease select a stream to view consumers"
	}

	c.messageLimit = height

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

	nameWidth := c.calculateColumnWidth("NAME", func(consumer models.Consumer) string { return consumer.Name })
	filterWidth := c.calculateColumnWidth("FILTER PATTERN", func(consumer models.Consumer) string { return consumer.FilterKeyPattern })

	var maxAckPendingWidth, maxDeliverWidth, createdWidth int
	showFullTable := width >= 100

	if showFullTable {
		createdWidth = c.calculateColumnWidth("CREATED AT", func(consumer models.Consumer) string { return consumer.CreatedAt.Format(time.RFC3339) })
		maxAckPendingWidth = c.calculateColumnWidth("MAX ACK PENDING", func(consumer models.Consumer) string { return fmt.Sprintf("%d", consumer.MaxAckPending) })
		maxDeliverWidth = c.calculateColumnWidth("MAX DELIVER", func(consumer models.Consumer) string { return fmt.Sprintf("%d", consumer.MaxDeliver) })
	}

	showDetailsPromptWidth := width - nameWidth - filterWidth - createdWidth
	if showFullTable {
		showDetailsPromptWidth -= maxAckPendingWidth + maxDeliverWidth + createdWidth
	}

	// Create the table header style
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray)).
		Width(width)

	// Add the "Consumers" title
	output := lipgloss.NewStyle().Bold(true).Render("Consumers") + "\n\n"

	// Format the table header
	var tableHeader string
	if showFullTable {
		tableHeader = fmt.Sprintf("%-*s %-*s %-*s %-*s %-*s %-*s",
			nameWidth, "NAME",
			filterWidth, "FILTER PATTERN",
			maxAckPendingWidth, "MAX ACK PENDING",
			maxDeliverWidth, "MAX DELIVER",
			createdWidth, "CREATED AT",
			showDetailsPromptWidth, "SHOW DETAILS")
	} else {
		tableHeader = fmt.Sprintf("%-*s %-*s %-*s",
			nameWidth, "NAME",
			filterWidth, "FILTER PATTERN",
			showDetailsPromptWidth, "SHOW DETAILS")
	}

	output += tableHeaderStyle.Render(tableHeader) + "\n"

	for _, consumer := range c.consumers {
		var line string
		if showFullTable {
			line = formatConsumerLine(consumer, nameWidth, filterWidth, maxAckPendingWidth, maxDeliverWidth, createdWidth)
		} else {
			line = formatConsumerLineSmall(consumer, nameWidth, filterWidth)
		}
		style := lipgloss.NewStyle().Width(width)
		showDetails := ""
		if consumer.ID == c.selectedConsumerID {
			style = style.
				Background(lipgloss.Color(colorPurple)).
				Foreground(lipgloss.Color(colorWhite))
			showDetails = " Press enter"
		}
		output += style.Render(line+fmt.Sprintf("%-*s", showDetailsPromptWidth, showDetails)) + "\n"
	}

	return output
}

func formatConsumerLineSmall(consumer models.Consumer, nameWidth, filterWidth int) string {
	name := truncateString(consumer.Name, nameWidth)
	filter := truncateString(consumer.FilterKeyPattern, filterWidth)

	return fmt.Sprintf("%-*s %-*s",
		nameWidth, name,
		filterWidth, filter)
}

func (c *ConsumerState) calculateColumnWidth(header string, getValue func(models.Consumer) string) int {
	maxWidth := len(header)
	for _, consumer := range c.consumers {
		value := getValue(consumer)
		if len(value) > maxWidth {
			maxWidth = len(value)
		}
	}
	return maxWidth
}

func formatConsumerLine(consumer models.Consumer, nameWidth, filterWidth, maxAckPendingWidth, maxDeliverWidth, createdWidth int) string {
	name := truncateString(consumer.Name, nameWidth)
	filter := truncateString(consumer.FilterKeyPattern, filterWidth)
	maxAckPending := fmt.Sprintf("%d", consumer.MaxAckPending)
	maxDeliver := fmt.Sprintf("%d", consumer.MaxDeliver)
	created := consumer.CreatedAt.Format(time.RFC3339)

	return fmt.Sprintf("%-*s %-*s %-*s %-*s %-*s",
		nameWidth, name,
		filterWidth, filter,
		maxAckPendingWidth, maxAckPending,
		maxDeliverWidth, maxDeliver,
		createdWidth, created)
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

	pendingHeaderHeight := lipgloss.Height(formatMessageSection("Ack pending messages", nil, width, true, false, false, 0))
	upcomingHeaderHeight := lipgloss.Height(formatMessageSection("Available messages", nil, width, false, false, false, 0))

	messageTableHeight := remainingHeight - pendingHeaderHeight - upcomingHeaderHeight

	pendingCount := len(c.pendingMessages)
	upcomingCount := len(c.upcomingMessages)
	totalCount := pendingCount + upcomingCount

	log.Println("pendingCount:", pendingCount, "upcomingCount:", upcomingCount, "totalCount:", totalCount, "messageTableHeight:", messageTableHeight)

	var pendingLimit, upcomingLimit int

	if totalCount > messageTableHeight {
		log.Println("totalCount > messageTableHeight")
		// If we have more messages than we can display, try to show an equal number of each
		pendingLimit = messageTableHeight / 2
		upcomingLimit = messageTableHeight - pendingLimit
	} else {
		log.Println("totalCount <= messageTableHeight")
		// If we have fewer messages than we can display, show all of them
		pendingLimit = pendingCount
		upcomingLimit = upcomingCount
	}

	// Adjust limits if one type has fewer messages than its limit
	if pendingCount < pendingLimit {
		log.Println("pendingCount < pendingLimit")
		upcomingLimit += pendingLimit - pendingCount
		pendingLimit = pendingCount
	} else if upcomingCount < upcomingLimit {
		log.Println("upcomingCount < upcomingLimit")
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
		return "No messages found.\n"
	}

	seqWidth := calculateColumnWidth(messages, "SEQ", func(msg models.ConsumerMessage) string {
		return fmt.Sprintf("%d", msg.MessageSeq)
	})
	keyWidth := calculateColumnWidth(messages, "KEY", func(msg models.ConsumerMessage) string {
		return msg.MessageKey
	})
	deliverCountWidth := calculateColumnWidth(messages, "DELIVER", func(msg models.ConsumerMessage) string {
		return fmt.Sprintf("%d", msg.DeliverCount)
	})

	lastColumnName := "Not Visible Until"
	if !isPending {
		lastColumnName = "CREATED AT"
	}
	lastColumnWidth := calculateColumnWidth(messages, lastColumnName, func(msg models.ConsumerMessage) string {
		if isPending {
			return msg.FormatNotVisibleUntil()
		}
		return msg.Message.CreatedAt.Format(time.RFC3339)
	})

	// Adjust column widths to fit the full width
	remainingWidth := width - seqWidth - keyWidth - deliverCountWidth - lastColumnWidth - 3 // 3 for spaces between columns
	if remainingWidth > 0 {
		keyWidth += remainingWidth // Add remaining width to the key column
	}

	header := formatMessageHeader(seqWidth, keyWidth, deliverCountWidth, lastColumnWidth, isPending, width)
	output := header

	for i, msg := range messages {
		style := lipgloss.NewStyle().Width(width)
		line := formatMessageRow(msg, seqWidth, keyWidth, deliverCountWidth, lastColumnWidth, isPending)
		if isSelected && i == cursor {
			style = style.Background(lipgloss.Color(colorPurple)).Foreground(lipgloss.Color(colorWhite))
		}
		output += style.Render(strings.TrimRight(line, "\n")) + "\n"
	}

	return output
}

func calculateColumnWidth(messages []models.ConsumerMessage, header string, getValue func(models.ConsumerMessage) string) int {
	maxWidth := len(header)
	for _, msg := range messages {
		value := getValue(msg)
		if len(value) > maxWidth {
			maxWidth = len(value)
		}
	}
	return maxWidth
}

func formatMessageHeader(seqWidth, keyWidth, deliverCountWidth, lastColumnWidth int, isPending bool, totalWidth int) string {
	lastColumnName := "Not Visible Until"
	if !isPending {
		lastColumnName = "CREATED AT"
	}

	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray)).
		Width(totalWidth)

	header := fmt.Sprintf("%-*s %-*s %-*s %-*s",
		seqWidth, "SEQ",
		keyWidth, "KEY",
		deliverCountWidth, "DELIVER",
		lastColumnWidth, lastColumnName)

	return tableHeaderStyle.Render(header) + "\n"
}

func formatMessageRow(msg models.ConsumerMessage, seqWidth, keyWidth, deliverCountWidth, lastColumnWidth int, isPending bool) string {
	lastColumn := msg.Message.CreatedAt.Format(time.RFC3339)
	if isPending {
		lastColumn = msg.FormatNotVisibleUntil()
	}

	return fmt.Sprintf("%-*d %-*s %-*d %-*s",
		seqWidth, msg.MessageSeq,
		keyWidth, truncateString(msg.MessageKey, keyWidth),
		deliverCountWidth, msg.DeliverCount,
		lastColumnWidth, lastColumn)
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
