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

	output := formatConsumerHeader(width)
	output += formatConsumerList(c.consumers, c.cursor, width)
	output += strings.Repeat("\n", height-len(c.consumers)-3)

	return output
}

func formatConsumerHeader(width int) string {
	header := fmt.Sprintf("\n%-36s | %-20s | %-25s | %-15s | %-15s | %-25s\n",
		"ID", "Slug", "Filter Pattern", "Max Ack Pending", "Max Deliver", "Created At")
	return header + strings.Repeat("-", width) + "\n"
}

func formatConsumerList(consumers []api.Consumer, cursor int, width int) string {
	var output string
	for i, consumer := range consumers {
		style := lipgloss.NewStyle()
		if i == cursor {
			style = style.Background(lipgloss.Color("25")).Foreground(lipgloss.Color("231"))
		}
		output += style.Render(fmt.Sprintf("%-36s | %-20s | %-25s | %-15d | %-15d | %-25s",
			consumer.ID,
			truncateString(consumer.Slug, 20),
			truncateString(consumer.FilterKeyPattern, 25),
			consumer.MaxAckPending,
			consumer.MaxDeliver,
			consumer.CreatedAt.Format(time.RFC3339)))
		output += "\n"
	}
	return output
}

func (c *Consumer) detailView(width, height int) string {
	consumer := c.consumers[c.cursor]
	output := formatConsumerDetail(consumer)

	output += formatMessageSection("Pending Messages", c.pendingMessages, width, true, c.isLoading)
	output += formatMessageSection("Next Messages", c.nextMessages, width, false, c.isLoading)

	return padOutput(output, height)
}

func formatConsumerDetail(consumer api.Consumer) string {
	output := lipgloss.NewStyle().Bold(true).Render("Consumer Detail")
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
	output := "\n" + lipgloss.NewStyle().Bold(true).Render(title) + "\n"
	if isLoading {
		return output + loadingSpinner()
	}
	return output + formatMessageList(messages, width, isPending)
}

func formatMessageList(messages []api.MessageWithInfo, width int, isPending bool) string {
	if width < 100 {
		return formatMessageListSmall(messages, width, isPending)
	}
	return formatMessageListLarge(messages, width, isPending)
}

func formatMessageListLarge(messages []api.MessageWithInfo, width int, isPending bool) string {
	if len(messages) == 0 {
		return "No messages found.\n"
	}

	seqWidth, keyWidth, deliverCountWidth := 10, width/3, 15
	lastColumnWidth := width - seqWidth - keyWidth - deliverCountWidth - 6

	header := formatMessageHeader(seqWidth, keyWidth, deliverCountWidth, lastColumnWidth, isPending)
	output := header + strings.Repeat("-", width) + "\n"

	for _, msg := range messages {
		output += formatMessageRow(msg, seqWidth, keyWidth, deliverCountWidth, lastColumnWidth, isPending)
	}

	return output
}

func formatMessageListSmall(messages []api.MessageWithInfo, width int, isPending bool) string {
	if len(messages) == 0 {
		return "No messages found.\n"
	}

	seqWidth, keyWidth := 10, 20
	lastColumnWidth := width - seqWidth - keyWidth - 3

	header := formatMessageHeader(seqWidth, keyWidth, 0, lastColumnWidth, isPending)
	output := header + strings.Repeat("-", width) + "\n"

	for _, msg := range messages {
		output += formatMessageRow(msg, seqWidth, keyWidth, 0, lastColumnWidth, isPending)
	}

	return output
}

func formatMessageHeader(seqWidth, keyWidth, deliverCountWidth, lastColumnWidth int, isPending bool) string {
	lastColumnName := "Not Visible Until"
	if !isPending {
		lastColumnName = "Created At"
	}

	if deliverCountWidth > 0 {
		return fmt.Sprintf("%-*s | %-*s | %-*s | %-*s\n",
			seqWidth, "Seq",
			keyWidth, "Key",
			deliverCountWidth, "Deliver Count",
			lastColumnWidth, lastColumnName)
	}
	return fmt.Sprintf("%-*s | %-*s | %-*s\n",
		seqWidth, "Seq",
		keyWidth, "Key",
		lastColumnWidth, lastColumnName)
}

func formatMessageRow(msg api.MessageWithInfo, seqWidth, keyWidth, deliverCountWidth, lastColumnWidth int, isPending bool) string {
	lastColumn := msg.Message.CreatedAt.Format(time.RFC3339)
	if isPending {
		lastColumn = msg.Info.FormatNotVisibleUntil()
	}

	if deliverCountWidth > 0 {
		return fmt.Sprintf("%-*d | %-*s | %-*d | %-*s\n",
			seqWidth, msg.Message.Seq,
			keyWidth, truncateString(msg.Message.Key, keyWidth),
			deliverCountWidth, msg.Info.DeliverCount,
			lastColumnWidth, lastColumn)
	}
	return fmt.Sprintf("%-*d | %-*s | %-*s\n",
		seqWidth, msg.Message.Seq,
		keyWidth, truncateString(msg.Message.Key, keyWidth),
		lastColumnWidth, lastColumn)
}

func padOutput(output string, height int) string {
	lines := strings.Count(output, "\n")
	return output + strings.Repeat("\n", height-lines)
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

func loadingSpinner() string {
	spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	return spinner[int(time.Now().UnixNano()/100000000)%len(spinner)] + " Loading...\n"
}
