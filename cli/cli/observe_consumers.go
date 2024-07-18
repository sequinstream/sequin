package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sequinstream/sequin/cli/api"
	sequinContext "github.com/sequinstream/sequin/cli/context"

	"github.com/charmbracelet/lipgloss"
)

type ConsumerState struct {
	consumers          []api.Consumer
	config             *Config
	selectedConsumerID string
	showDetail         bool
	pendingMessages    []api.MessageWithInfo
	upcomingMessages   []api.MessageWithInfo
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
	return nil
}

func limitConsumers(consumers []api.Consumer, limit int) []api.Consumer {
	if len(consumers) > limit {
		return consumers[:limit]
	}
	return consumers
}

func (c *ConsumerState) updateDetailView() {
	if len(c.consumers) == 1 {
		c.setSelectedConsumer(c.consumers[0].ID)
		c.showDetail = true
	} else if len(c.consumers) == 0 {
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

func (c *ConsumerState) View(width, height int) string {
	if c.streamName == "" {
		return "\nPlease select a stream to view consumers"
	}

	if c.showDetail || len(c.consumers) == 1 {
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

	nameWidth := c.calculateNameWidth(width)
	filterWidth := 25
	maxAckPendingWidth := 15
	maxDeliverWidth := 15
	createdWidth := 25
	showDetailsPromptWidth := width - filterWidth - maxAckPendingWidth - maxDeliverWidth - createdWidth

	// Create the table header style
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("0")). // Black text
		Background(lipgloss.Color("2")). // Green background
		Width(width)

	// Add the "CONSUMERS" title
	output := lipgloss.NewStyle().Bold(true).Render("CONSUMERS") + "\n\n"

	// Format the table header
	tableHeader := fmt.Sprintf("%-*s %-*s %-*s %-*s %-*s %-*s",
		nameWidth, "SLUG",
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
				Background(lipgloss.Color("117")). // Light blue background
				Foreground(lipgloss.Color("0"))    // Black text
			showDetails = "Press enter"
		}
		output += style.Render(line+fmt.Sprintf(" %-*s", showDetailsPromptWidth, showDetails)) + "\n"
	}

	return output
}

func (c *ConsumerState) calculateNameWidth(totalWidth int) int {
	maxNameWidth := 3 // Minimum width for "Name" header
	for _, consumer := range c.consumers {
		nameWidth := len(consumer.Name)
		if nameWidth > maxNameWidth {
			maxNameWidth = nameWidth
		}
	}
	return min(min(maxNameWidth, totalWidth/4), 255)
}

func formatConsumerLine(consumer api.Consumer, nameWidth, filterWidth, maxAckPendingWidth, maxDeliverWidth, createdWidth int) string {
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

	output += formatMessageSection("Pending Messages", c.pendingMessages, width, true, c.isLoading)
	output += formatMessageSection("Upcoming Messages", c.upcomingMessages, width, false, c.isLoading)

	return output
}

func (c *ConsumerState) getSelectedConsumer() *api.Consumer {
	for _, consumer := range c.consumers {
		if consumer.ID == c.selectedConsumerID {
			return &consumer
		}
	}
	return nil
}

func formatConsumerDetail(consumer api.Consumer) string {
	output := lipgloss.NewStyle().Bold(true).Render("CONSUMER DETAIL")
	output += "\n\n"
	output += fmt.Sprintf("ID:              %s\n", consumer.ID)
	output += fmt.Sprintf("Name:            %s\n", consumer.Name)
	output += fmt.Sprintf("Filter:          %s\n", consumer.FilterKeyPattern)
	output += fmt.Sprintf("Max Ack Pending: %d\n", consumer.MaxAckPending)
	output += fmt.Sprintf("Max Deliver:     %d\n", consumer.MaxDeliver)
	output += fmt.Sprintf("Created At:      %s\n", consumer.CreatedAt.Format(time.RFC3339))
	return output
}

func formatMessageSection(title string, messages []api.MessageWithInfo, width int, isPending, isLoading bool) string {
	// Create the table header style
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Width(width)

	output := "\n" + tableHeaderStyle.Render(title) + "\n"
	if isLoading {
		return output + loadingSpinner()
	}
	return output + formatMessageList(messages, width, isPending)
}

func formatMessageList(messages []api.MessageWithInfo, width int, isPending bool) string {
	seqWidth := calculateSeqWidth(messages)

	if width < 80 {
		return formatMessageListSmall(messages, width, isPending, seqWidth)
	}
	return formatMessageListLarge(messages, width, isPending, seqWidth)
}

func calculateSeqWidth(messages []api.MessageWithInfo) int {
	maxSeqWidth := 3 // Minimum width for "SEQ" header
	for _, msg := range messages {
		seqWidth := len(fmt.Sprintf("%d", msg.Message.Seq))
		if seqWidth > maxSeqWidth {
			maxSeqWidth = seqWidth
		}
	}
	return maxSeqWidth
}

func formatMessageListLarge(messages []api.MessageWithInfo, width int, isPending bool, seqWidth int) string {
	if len(messages) == 0 {
		return "No messages found.\n"
	}

	keyWidth, deliverCountWidth := width/3, 7
	lastColumnWidth := width - seqWidth - keyWidth - deliverCountWidth - 6

	header := formatMessageHeader(seqWidth, keyWidth, deliverCountWidth, lastColumnWidth, isPending)
	output := header

	for _, msg := range messages {
		output += formatMessageRow(msg, seqWidth, keyWidth, deliverCountWidth, lastColumnWidth, isPending)
	}

	return output
}

func formatMessageListSmall(messages []api.MessageWithInfo, width int, isPending bool, seqWidth int) string {
	if len(messages) == 0 {
		return "No messages found.\n"
	}

	keyWidth := 20
	lastColumnWidth := width - seqWidth - keyWidth - 3

	header := formatMessageHeader(seqWidth, keyWidth, 0, lastColumnWidth, isPending)
	output := header

	for _, msg := range messages {
		output += formatMessageRow(msg, seqWidth, keyWidth, 0, lastColumnWidth, isPending)
	}

	return output
}

func formatMessageHeader(seqWidth, keyWidth, deliverCountWidth, lastColumnWidth int, isPending bool) string {
	lastColumnName := "Not Visible Until"
	if !isPending {
		lastColumnName = "CREATED AT"
	}

	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("0")). // Black text
		Background(lipgloss.Color("2"))  // Green background

	var header string
	if deliverCountWidth > 0 {
		header = fmt.Sprintf("%-*s %-*s %-*s %-*s",
			seqWidth, "SEQ",
			keyWidth, "KEY",
			deliverCountWidth, "DELIVER",
			lastColumnWidth, lastColumnName)
	} else {
		header = fmt.Sprintf("%-*s %-*s %-*s",
			seqWidth, "SEQ",
			keyWidth, "KEY",
			lastColumnWidth, lastColumnName)
	}

	return tableHeaderStyle.Render(header) + "\n"
}

func formatMessageRow(msg api.MessageWithInfo, seqWidth, keyWidth, deliverCountWidth, lastColumnWidth int, isPending bool) string {
	lastColumn := msg.Message.CreatedAt.Format(time.RFC3339)
	if isPending {
		lastColumn = msg.Info.FormatNotVisibleUntil()
	}

	if deliverCountWidth > 0 {
		return fmt.Sprintf("%-*d %-*s %-*d %-*s\n",
			seqWidth, msg.Message.Seq,
			keyWidth, truncateString(msg.Message.Key, keyWidth),
			deliverCountWidth, msg.Info.DeliverCount,
			lastColumnWidth, lastColumn)
	}
	return fmt.Sprintf("%-*d %-*s %-*s\n",
		seqWidth, msg.Message.Seq,
		keyWidth, truncateString(msg.Message.Key, keyWidth),
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

func (c *ConsumerState) fetchMessages(consumerID string, visible bool) ([]api.MessageWithInfo, error) {
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
