package cli

import (
	"fmt"
	"strings"
	"time"

	"sequin-cli/api"
	"sequin-cli/context"

	"github.com/charmbracelet/lipgloss"
)

type Consumer struct {
	consumers  []api.Consumer
	config     *Config
	cursor     int
	showDetail bool
}

func NewConsumer(config *Config) *Consumer {
	return &Consumer{
		config:     config,
		cursor:     0,
		showDetail: false,
	}
}

func (c *Consumer) FetchConsumers(limit int) error {
	ctx, err := context.LoadContext(c.config.ContextName)
	if err != nil {
		return err
	}

	streamID, err := getFirstStream(ctx)
	if err != nil {
		return err
	}

	consumers, err := api.FetchConsumers(ctx, streamID)
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
	maxAckPendingWidth := 15
	maxDeliverWidth := 15
	createdWidth := 25

	output := fmt.Sprintf("\n%-*s | %-*s | %-*s | %-*s | %-*s\n",
		idWidth, "ID",
		slugWidth, "Slug",
		maxAckPendingWidth, "Max Ack Pending",
		maxDeliverWidth, "Max Deliver",
		createdWidth, "Created At")
	output += strings.Repeat("-", width) + "\n"

	for i, consumer := range c.consumers {
		style := lipgloss.NewStyle()
		if i == c.cursor {
			style = style.Background(lipgloss.Color("25")).Foreground(lipgloss.Color("231"))
		}
		output += style.Render(fmt.Sprintf("%-*s | %-*s | %-*d | %-*d | %-*s",
			idWidth, consumer.ID,
			slugWidth, truncateString(consumer.Slug, slugWidth),
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
	output := lipgloss.NewStyle().Bold(true).Render("Consumer Detail\n\n")
	output += fmt.Sprintf("ID: %s\n", consumer.ID)
	output += fmt.Sprintf("Slug: %s\n", consumer.Slug)
	output += fmt.Sprintf("Max Ack Pending: %d\n", consumer.MaxAckPending)
	output += fmt.Sprintf("Max Deliver: %d\n", consumer.MaxDeliver)
	output += fmt.Sprintf("Created At: %s\n", consumer.CreatedAt.Format(time.RFC3339))

	// Pad the output to fill the available height
	lines := strings.Count(output, "\n")
	for i := 0; i < height-lines; i++ {
		output += "\n"
	}

	return output
}

func (c *Consumer) ToggleDetail() {
	c.showDetail = !c.showDetail
}

func (c *Consumer) MoveCursor(direction int) {
	c.cursor += direction
	if c.cursor < 0 {
		c.cursor = 0
	} else if c.cursor >= len(c.consumers) {
		c.cursor = len(c.consumers) - 1
	}
}
