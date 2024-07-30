package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sequinstream/sequin/cli/api"
	sequinContext "github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"

	"github.com/charmbracelet/lipgloss"
)

type ConsumerState struct {
	consumers          []models.Consumer
	config             *Config
	selectedConsumerID string
	showDetail         bool
	pendingMessages    []models.MessageWithInfo
	upcomingMessages   []models.MessageWithInfo
	streamName         string
	ctx                *sequinContext.Context
	isLoading          bool
}

func NewConsumerState(config *Config, ctx *sequinContext.Context) *ConsumerState {
	return &ConsumerState{
		config: config,
		ctx:    ctx,
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
		c.showDetail = false
	} else if c.selectedConsumerID == "" {
		c.showDetail = false
	}
}

func (c *ConsumerState) setSelectedConsumer(id string) {
	if c.selectedConsumerID != id {
		c.selectedConsumerID = id
		c.resetMessages()
		c.isLoading = true
	}
}

func (c *ConsumerState) WillAppear(limit int) {
	c.FetchConsumers(limit)
}

func (c *ConsumerState) View(width, height int) string {
	if c.streamName == "" {
		return "\nPlease select a stream to view consumers"
	}

	if c.showDetail {
		return c.detailView(width)
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
	maxAckPendingWidth := c.calculateColumnWidth("MAX ACK PENDING", func(consumer models.Consumer) string { return fmt.Sprintf("%d", consumer.MaxAckPending) })
	maxDeliverWidth := c.calculateColumnWidth("MAX DELIVER", func(consumer models.Consumer) string { return fmt.Sprintf("%d", consumer.MaxDeliver) })
	createdWidth := c.calculateColumnWidth("CREATED AT", func(consumer models.Consumer) string { return consumer.CreatedAt.Format(time.RFC3339) })
	showDetailsPromptWidth := width - nameWidth - filterWidth - maxAckPendingWidth - maxDeliverWidth - createdWidth

	// Create the table header style
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray)).
		Width(width)

	// Add the "CONSUMERS" title
	output := lipgloss.NewStyle().Bold(true).Render("Select a consumer to view details") + "\n\n"

	// Format the table header
	tableHeader := fmt.Sprintf("%-*s %-*s %-*s %-*s %-*s %-*s",
		nameWidth, "NAME",
		filterWidth, "FILTER PATTERN",
		maxAckPendingWidth, "MAX ACK PENDING",
		maxDeliverWidth, "MAX DELIVER",
		createdWidth, "CREATED AT",
		showDetailsPromptWidth, "SHOW DETAILS")

	output += tableHeaderStyle.Render(tableHeader) + "\n"

	for _, consumer := range c.consumers {
		line := formatConsumerLine(consumer, nameWidth, filterWidth, maxAckPendingWidth, maxDeliverWidth, createdWidth)
		style := lipgloss.NewStyle()
		showDetails := ""
		if consumer.ID == c.selectedConsumerID {
			style = style.
				Background(lipgloss.Color("57")). // Purple background
				Foreground(lipgloss.Color("255")) // White text
			showDetails = "Press enter"
		}
		output += style.Render(line+fmt.Sprintf(" %-*s", showDetailsPromptWidth, showDetails)) + "\n"
	}

	return output
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

func (c *ConsumerState) detailView(width int) string {
	if len(c.consumers) == 0 || c.selectedConsumerID == "" {
		return "No consumer selected"
	}

	consumer := c.getSelectedConsumer()
	if consumer == nil {
		return "Selected consumer not found"
	}

	output := formatConsumerDetail(*consumer)

	output += formatMessageSection("Pending messages", c.pendingMessages, width, true, c.isLoading)
	output += formatMessageSection("Upcoming messages", c.upcomingMessages, width, false, c.isLoading)

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

func formatConsumerDetail(consumer models.Consumer) string {
	output := lipgloss.NewStyle().Bold(true).Render("Consumer details")
	output += "\n\n"
	output += fmt.Sprintf("ID:              %s\n", consumer.ID)
	output += fmt.Sprintf("Name:            %s\n", consumer.Name)
	output += fmt.Sprintf("Filter:          %s\n", consumer.FilterKeyPattern)
	output += fmt.Sprintf("Max Ack Pending: %d\n", consumer.MaxAckPending)
	output += fmt.Sprintf("Max Deliver:     %d\n", consumer.MaxDeliver)
	output += fmt.Sprintf("Created At:      %s\n", consumer.CreatedAt.Format(time.RFC3339))
	return output
}

func formatMessageSection(title string, messages []models.MessageWithInfo, width int, isPending, isLoading bool) string {
	headerStyle := lipgloss.NewStyle().
		Bold(true)

	output := "\n" + headerStyle.Render(title) + "\n"
	if isLoading {
		return output + loadingSpinner()
	}
	return output + formatMessageList(messages, width, isPending)
}

func formatMessageList(messages []models.MessageWithInfo, _ int, isPending bool) string {
	if len(messages) == 0 {
		return "No messages found.\n"
	}

	seqWidth := calculateColumnWidth(messages, "SEQ", func(msg models.MessageWithInfo) string {
		return fmt.Sprintf("%d", msg.Message.Seq)
	})
	keyWidth := calculateColumnWidth(messages, "KEY", func(msg models.MessageWithInfo) string {
		return msg.Message.Key
	})
	deliverCountWidth := calculateColumnWidth(messages, "DELIVER", func(msg models.MessageWithInfo) string {
		return fmt.Sprintf("%d", msg.Info.DeliverCount)
	})

	lastColumnName := "Not Visible Until"
	if !isPending {
		lastColumnName = "CREATED AT"
	}
	lastColumnWidth := calculateColumnWidth(messages, lastColumnName, func(msg models.MessageWithInfo) string {
		if isPending {
			return msg.Info.FormatNotVisibleUntil()
		}
		return msg.Message.CreatedAt.Format(time.RFC3339)
	})

	header := formatMessageHeader(seqWidth, keyWidth, deliverCountWidth, lastColumnWidth, isPending)
	output := header

	for _, msg := range messages {
		output += formatMessageRow(msg, seqWidth, keyWidth, deliverCountWidth, lastColumnWidth, isPending)
	}

	return output
}

func calculateColumnWidth(messages []models.MessageWithInfo, header string, getValue func(models.MessageWithInfo) string) int {
	maxWidth := len(header)
	for _, msg := range messages {
		value := getValue(msg)
		if len(value) > maxWidth {
			maxWidth = len(value)
		}
	}
	return maxWidth
}

func formatMessageHeader(seqWidth, keyWidth, deliverCountWidth, lastColumnWidth int, isPending bool) string {
	lastColumnName := "Not Visible Until"
	if !isPending {
		lastColumnName = "CREATED AT"
	}

	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(colorBlack)).
		Background(lipgloss.Color(colorLightGray))

	header := fmt.Sprintf("%-*s %-*s %-*s %-*s",
		seqWidth, "SEQ",
		keyWidth, "KEY",
		deliverCountWidth, "DELIVER",
		lastColumnWidth, lastColumnName)

	return tableHeaderStyle.Render(header) + "\n"
}

func formatMessageRow(msg models.MessageWithInfo, seqWidth, keyWidth, deliverCountWidth, lastColumnWidth int, isPending bool) string {
	lastColumn := msg.Message.CreatedAt.Format(time.RFC3339)
	if isPending {
		lastColumn = msg.Info.FormatNotVisibleUntil()
	}

	return fmt.Sprintf("%-*d %-*s %-*d %-*s\n",
		seqWidth, msg.Message.Seq,
		keyWidth, truncateString(msg.Message.Key, keyWidth),
		deliverCountWidth, msg.Info.DeliverCount,
		lastColumnWidth, lastColumn)
}

func (c *ConsumerState) ToggleDetail() {
	if c.showDetail {
		c.showDetail = false
	} else if c.selectedConsumerID != "" {
		c.showDetail = true
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

func (c *ConsumerState) fetchPendingAndUpcomingMessages() error {
	if c.streamName == "" || !c.showDetail || len(c.consumers) == 0 {
		return nil
	}

	consumer := c.getSelectedConsumer()
	if !c.showDetail || c.selectedConsumerID == "" {
		return nil
	}

	pending, err := c.fetchMessages(consumer.ID, false)
	if err != nil {
		return fmt.Errorf("failed to fetch pending messages: %w", err)
	}
	c.pendingMessages = pending

	upcoming, err := c.fetchMessages(consumer.ID, true)
	if err != nil {
		return fmt.Errorf("failed to fetch upcoming messages: %w", err)
	}
	c.upcomingMessages = upcoming

	c.isLoading = false // Set isLoading to false after fetching messages
	return nil
}

func (c *ConsumerState) fetchMessages(consumerID string, visible bool) ([]models.MessageWithInfo, error) {
	options := api.FetchMessagesOptions{
		StreamIDOrName: c.streamName,
		ConsumerID:     consumerID,
		Visible:        visible,
		Limit:          10,
		Order:          "seq_asc",
	}
	return api.FetchMessages(c.ctx, options)
}

func (c *ConsumerState) StartMessageUpdates(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.fetchPendingAndUpcomingMessages(); err != nil {
					fmt.Printf("Error updating messages: %v\n", err)
				}
			}
		}
	}()
}

func (c *ConsumerState) DisableDetailView() {
	c.showDetail = false
}

func (c *ConsumerState) SetStreamName(streamName string) {
	c.streamName = streamName
	c.consumers = nil
	c.selectedConsumerID = ""
	c.showDetail = false
	c.pendingMessages = nil
	c.upcomingMessages = nil
	c.isLoading = false
}
