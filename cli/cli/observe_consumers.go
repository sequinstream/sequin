package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/sequinstream/sequin/cli/api"
	sequinContext "github.com/sequinstream/sequin/cli/context"

	"github.com/charmbracelet/lipgloss"
)

type Consumer struct {
	consumers       []api.Consumer
	config          *Config
	cursor          int
	showDetail      bool
	pendingMessages []api.MessageWithInfo
	nextMessages    []api.MessageWithInfo
	streamID        string
	ctx             *sequinContext.Context
	isLoading       bool
}

func NewConsumer(config *Config, ctx *sequinContext.Context) *Consumer {
	return &Consumer{
		config: config,
		ctx:    ctx,
	}
}

func (c *Consumer) FetchConsumers(limit int) error {
	if c.ctx == nil {
		return fmt.Errorf("context is not set")
	}

	streamID, err := getFirstStream(c.ctx)
	if err != nil {
		return err
	}
	c.streamID = streamID

	consumers, err := api.FetchConsumers(c.ctx, streamID)
	if err != nil {
		return err
	}

	c.consumers = limitConsumers(consumers, limit)
	return nil
}

func limitConsumers(consumers []api.Consumer, limit int) []api.Consumer {
	if len(consumers) > limit {
		return consumers[:limit]
	}
	return consumers
}

func (c *Consumer) View(width, height int) string {
	if c.showDetail {
		return c.detailView(width, height)
	}
	return c.listView(width, height)
}

func (c *Consumer) listView(width, height int) string {
	if len(c.consumers) == 0 {
		return "\nNo consumers found."
	}

	slugWidth := c.calculateSlugWidth(width)
	filterWidth := 25
	maxAckPendingWidth := 15
	maxDeliverWidth := 15
	createdWidth := 25

	// Create the table header style
	tableHeaderStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("0")). // Black text
		Background(lipgloss.Color("2")). // Green background
		Width(width)

	// Add the "CONSUMERS" title
	output := lipgloss.NewStyle().Bold(true).Render("CONSUMERS") + "\n\n"

	// Format the table header
	tableHeader := fmt.Sprintf("%-*s %-*s %-*s %-*s %-*s",
		slugWidth, "SLUG",
		filterWidth, "FILTER PATTERN",
		maxAckPendingWidth, "MAX ACK PENDING",
		maxDeliverWidth, "MAX DELIVER",
		createdWidth, "CREATED AT")

	output += tableHeaderStyle.Render(tableHeader) + "\n"

	for i, consumer := range c.consumers {
		line := formatConsumerLine(consumer, slugWidth, filterWidth, maxAckPendingWidth, maxDeliverWidth, createdWidth)
		style := lipgloss.NewStyle()
		if i == c.cursor {
			style = style.
				Background(lipgloss.Color("117")). // Light blue background
				Foreground(lipgloss.Color("0"))    // Black text
		}
		output += style.Render(line) + "\n"
	}

	return output
}

func (c *Consumer) calculateSlugWidth(totalWidth int) int {
	maxSlugWidth := 3 // Minimum width for "Slug" header
	for _, consumer := range c.consumers {
		slugWidth := len(consumer.Slug)
		if slugWidth > maxSlugWidth {
			maxSlugWidth = slugWidth
		}
	}
	return min(min(maxSlugWidth, totalWidth/4), 255)
}

func formatConsumerLine(consumer api.Consumer, slugWidth, filterWidth, maxAckPendingWidth, maxDeliverWidth, createdWidth int) string {
	slug := truncateString(consumer.Slug, slugWidth)
	filter := truncateString(consumer.FilterKeyPattern, filterWidth)
	maxAckPending := fmt.Sprintf("%d", consumer.MaxAckPending)
	maxDeliver := fmt.Sprintf("%d", consumer.MaxDeliver)
	created := consumer.CreatedAt.Format(time.RFC3339)

	return fmt.Sprintf("%-*s %-*s %-*s %-*s %-*s",
		slugWidth, slug,
		filterWidth, filter,
		maxAckPendingWidth, maxAckPending,
		maxDeliverWidth, maxDeliver,
		createdWidth, created)
}

func (c *Consumer) detailView(width, height int) string {
	if len(c.consumers) == 0 || c.cursor < 0 || c.cursor >= len(c.consumers) {
		return "No consumer selected"
	}

	consumer := c.consumers[c.cursor]
	output := formatConsumerDetail(consumer)

	output += formatMessageSection("Pending Messages", c.pendingMessages, width, true, c.isLoading)
	output += formatMessageSection("Next Messages", c.nextMessages, width, false, c.isLoading)

	return output
}

func formatConsumerDetail(consumer api.Consumer) string {
	output := lipgloss.NewStyle().Bold(true).Render("CONSUMER DETAIL")
	output += "\n\n"
	output += fmt.Sprintf("ID:              %s\n", consumer.ID)
	output += fmt.Sprintf("Slug:            %s\n", consumer.Slug)
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

func (c *Consumer) ToggleDetail() {
	c.showDetail = !c.showDetail
}

func (c *Consumer) MoveCursor(direction int) {
	oldCursor := c.cursor
	c.cursor = clamp(c.cursor+direction, 0, len(c.consumers)-1)

	if oldCursor != c.cursor {
		c.resetMessages()
	}
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

func (c *Consumer) resetMessages() {
	c.pendingMessages = nil
	c.nextMessages = nil
	c.isLoading = true
}

func (c *Consumer) FetchPendingAndNextMessages() error {
	if !c.showDetail || len(c.consumers) == 0 {
		return nil
	}

	consumer := c.consumers[c.cursor]

	pending, err := c.fetchMessages(consumer.ID, false)
	if err != nil {
		return fmt.Errorf("failed to fetch pending messages: %w", err)
	}
	c.pendingMessages = pending

	next, err := c.fetchMessages(consumer.ID, true)
	if err != nil {
		return fmt.Errorf("failed to fetch next messages: %w", err)
	}
	c.nextMessages = next

	c.isLoading = false

	return nil
}

func (c *Consumer) fetchMessages(consumerID string, visible bool) ([]api.MessageWithInfo, error) {
	options := api.FetchMessagesOptions{
		StreamID:   c.streamID,
		ConsumerID: consumerID,
		Visible:    visible,
		Limit:      10,
		Order:      "seq_asc",
	}
	return api.FetchMessages(c.ctx, options)
}

func (c *Consumer) StartMessageUpdates(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.FetchPendingAndNextMessages(); err != nil {
					fmt.Printf("Error updating messages: %v\n", err)
				}
			}
		}
	}()
}

func (c *Consumer) DisableDetailView() {
	c.showDetail = false
}
