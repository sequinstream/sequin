package cli

import (
	"fmt"
	"strings"
	"time"

	"sequin-cli/api"
	"sequin-cli/context"
)

type Consumer struct {
	consumers []api.Consumer
	config    *Config
}

func NewConsumer(config *Config) *Consumer {
	return &Consumer{
		config: config,
	}
}

func (c *Consumer) FetchConsumers() error {
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

	c.consumers = consumers
	return nil
}

func (c *Consumer) View(width, height int) string {
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

	for _, consumer := range c.consumers {
		output += fmt.Sprintf("%-*s | %-*s | %-*d | %-*d | %-*s\n",
			idWidth, consumer.ID,
			slugWidth, truncateString(consumer.Slug, slugWidth),
			maxAckPendingWidth, consumer.MaxAckPending,
			maxDeliverWidth, consumer.MaxDeliver,
			createdWidth, consumer.CreatedAt.Format(time.RFC3339))
	}

	for i := 0; i < height-len(c.consumers)-3; i++ {
		output += "\n"
	}

	return output
}
