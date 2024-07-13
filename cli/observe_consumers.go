package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sequin-cli/api"
	sequinContext "sequin-cli/context"

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
		config:          config,
		cursor:          0,
		showDetail:      false,
		pendingMessages: []api.MessageWithInfo{},
		nextMessages:    []api.MessageWithInfo{},
		ctx:             ctx,
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

	// Limit the number of consumers to the specified limit
	if len(consumers) > limit {
		consumers = consumers[:limit]
	}

	c.consumers = consumers
	return nil
}

func (c *Consumer) View(width, height int) string {
	if c.showDetail {
		return c.detailView(width, height)
	}

	if len(c.consumers) == 0 {
		return "\nNo consumers found."
	}

	idWidth := 36
	slugWidth := 20
	filterPatternWidth := 25
	maxAckPendingWidth := 15
	maxDeliverWidth := 15
	createdWidth := 25

	output := fmt.Sprintf("\n%-*s | %-*s | %-*s | %-*s | %-*s | %-*s\n",
		idWidth, "ID",
		slugWidth, "Slug",
		filterPatternWidth, "Filter Pattern",
		maxAckPendingWidth, "Max Ack Pending",
		maxDeliverWidth, "Max Deliver",
		createdWidth, "Created At")
	output += strings.Repeat("-", width) + "\n"

	for i, consumer := range c.consumers {
		style := lipgloss.NewStyle()
		if i == c.cursor {
			style = style.Background(lipgloss.Color("25")).Foreground(lipgloss.Color("231"))
		}
		output += style.Render(fmt.Sprintf("%-*s | %-*s | %-*s | %-*d | %-*d | %-*s",
			idWidth, consumer.ID,
			slugWidth, truncateString(consumer.Slug, slugWidth),
			filterPatternWidth, truncateString(consumer.FilterKeyPattern, filterPatternWidth),
			maxAckPendingWidth, consumer.MaxAckPending,
			maxDeliverWidth, consumer.MaxDeliver,
			createdWidth, consumer.CreatedAt.Format(time.RFC3339)))
		output += "\n"
	}

	for i := 0; i < height-len(c.consumers)-3; i++ {
		output += "\n"
	}

	return output
}

func (c *Consumer) detailView(width, height int) string {
	consumer := c.consumers[c.cursor]
	output := lipgloss.NewStyle().Bold(true).Render("Consumer Detail")
	output += "\n\n"
	output += fmt.Sprintf("ID:              %s\n", consumer.ID)
	output += fmt.Sprintf("Slug:            %s\n", consumer.Slug)
	output += fmt.Sprintf("Filter:          %s\n", consumer.FilterKeyPattern)
	output += fmt.Sprintf("Max Ack Pending: %d\n", consumer.MaxAckPending)
	output += fmt.Sprintf("Max Deliver:     %d\n", consumer.MaxDeliver)
	output += fmt.Sprintf("Created At:      %s\n", consumer.CreatedAt.Format(time.RFC3339))

	output += "\n" + lipgloss.NewStyle().Bold(true).Render("Pending Messages") + "\n"
	if c.isLoading {
		output += c.loadingSpinner()
	} else {
		output += c.formatMessageList(c.pendingMessages, width, true)
	}

	output += "\n" + lipgloss.NewStyle().Bold(true).Render("Next Messages") + "\n"
	if c.isLoading {
		output += c.loadingSpinner()
	} else {
		output += c.formatMessageList(c.nextMessages, width, false)
	}

	// Pad the output to fill the available height
	lines := strings.Count(output, "\n")
	for i := 0; i < height-lines; i++ {
		output += "\n"
	}

	return output
}

func (c *Consumer) formatMessageList(messages []api.MessageWithInfo, width int, isPending bool) string {
	if width < 100 {
		return c.formatMessageListSmall(messages, width, isPending)
	}

	if messages == nil {
		return "No messages.\n"
	}
	if len(messages) == 0 {
		return "No messages found.\n"
	}

	seqWidth := 10
	keyWidth := width / 3
	deliverCountWidth := 15
	lastColumnWidth := width - seqWidth - keyWidth - deliverCountWidth - 6 // 6 for separators

	var header string
	if isPending {
		header = fmt.Sprintf("%-*s | %-*s | %-*s | %-*s\n",
			seqWidth, "Seq",
			keyWidth, "Key",
			deliverCountWidth, "Deliver Count",
			lastColumnWidth, "Not Visible Until")
	} else {
		header = fmt.Sprintf("%-*s | %-*s | %-*s | %-*s\n",
			seqWidth, "Seq",
			keyWidth, "Key",
			deliverCountWidth, "Deliver Count",
			lastColumnWidth, "Created At")
	}

	output := header
	output += strings.Repeat("-", width) + "\n"

	for _, msg := range messages {
		info := msg.Info
		var lastColumn string
		if isPending {
			lastColumn = info.FormatNotVisibleUntil()
		} else {
			lastColumn = msg.Message.CreatedAt.Format(time.RFC3339)
		}

		output += fmt.Sprintf("%-*d | %-*s | %-*d | %-*s\n",
			seqWidth, msg.Message.Seq,
			keyWidth, truncateString(msg.Message.Key, keyWidth),
			deliverCountWidth, info.DeliverCount,
			lastColumnWidth, lastColumn)
	}

	return output
}

func (c *Consumer) formatMessageListSmall(messages []api.MessageWithInfo, width int, isPending bool) string {
	if messages == nil {
		return "No messages.\n"
	}
	if len(messages) == 0 {
		return "No messages found.\n"
	}

	seqWidth := 10
	keyWidth := 20
	lastColumnWidth := width - seqWidth - keyWidth - 3 // 3 for separators

	var header string
	if isPending {
		header = fmt.Sprintf("%-*s | %-*s | %-*s\n", seqWidth, "Seq", keyWidth, "Key", lastColumnWidth, "Not Visible Until")
	} else {
		header = fmt.Sprintf("%-*s | %-*s | %-*s\n", seqWidth, "Seq", keyWidth, "Key", lastColumnWidth, "Created At")
	}

	output := header
	output += strings.Repeat("-", width) + "\n"

	for _, msg := range messages {
		info := msg.Info
		var lastColumn string
		if isPending {
			lastColumn = info.FormatNotVisibleUntil()
		} else {
			lastColumn = msg.Message.CreatedAt.Format(time.RFC3339)
		}

		output += fmt.Sprintf("%-*d | %-*s | %-*s\n",
			seqWidth, msg.Message.Seq,
			keyWidth, truncateString(msg.Message.Key, keyWidth),
			lastColumnWidth, lastColumn)
	}

	return output
}

func (c *Consumer) ToggleDetail() {
	c.showDetail = !c.showDetail
}

func (c *Consumer) MoveCursor(direction int) {
	oldCursor := c.cursor
	c.cursor += direction
	if c.cursor < 0 {
		c.cursor = 0
	} else if c.cursor >= len(c.consumers) {
		c.cursor = len(c.consumers) - 1
	}

	if oldCursor != c.cursor {
		c.pendingMessages = nil
		c.nextMessages = nil
		c.isLoading = true
	}
}

func (c *Consumer) FetchPendingAndNextMessages() error {
	if !c.showDetail || len(c.consumers) == 0 {
		return nil
	}
	consumer := c.consumers[c.cursor]

	// Fetch pending messages
	pendingOptions := api.FetchMessagesOptions{
		StreamID:   c.streamID,
		ConsumerID: consumer.ID,
		Visible:    false,
		Limit:      10,
		Order:      "seq_asc",
	}
	pending, err := api.FetchMessages(c.ctx, pendingOptions)
	if err != nil {
		return fmt.Errorf("failed to fetch pending messages: %w", err)
	}
	c.pendingMessages = pending

	// Fetch next messages
	nextOptions := api.FetchMessagesOptions{
		StreamID:   c.streamID,
		ConsumerID: consumer.ID,
		Visible:    true,
		Limit:      10,
		Order:      "seq_asc",
	}
	next, err := api.FetchMessages(c.ctx, nextOptions)
	if err != nil {
		return fmt.Errorf("failed to fetch next messages: %w", err)
	}
	c.nextMessages = next

	c.isLoading = false

	return nil
}

// Add this method to continuously update the messages
func (c *Consumer) StartMessageUpdates(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := c.FetchPendingAndNextMessages()
				if err != nil {
					fmt.Printf("Error updating messages: %v\n", err)
				}
			}
		}
	}()
}

func (c *Consumer) loadingSpinner() string {
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	return spinner[int(time.Now().UnixNano()/100000000)%len(spinner)] + " Loading...\n"
}
