package cli

import (
	"fmt"
	"strconv"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"github.com/google/go-cmp/cmp"

	"sequin-cli/api"
	"sequin-cli/context"
)

type consumerConfig struct {
	StreamID             string
	ConsumerID           string
	Slug                 string
	AckWaitMS            int
	MaxAckPending        int
	MaxDeliver           int
	MaxWaiting           int
	FilterSubjectPattern string
	BatchSize            int
	NoAck                bool
	PendingOnly          bool
	LastN                int
	FirstN               int
	AckID                string
	Force                bool
	UseDefaults          bool
}

func AddConsumerCommands(app *fisk.Application, config *Config) {
	consumer := app.Command("consumer", "Consumer related commands")

	addCheat("consumer", consumer)

	c := &consumerConfig{}

	lsCmd := consumer.Command("ls", "List consumers for a stream").Action(func(ctx *fisk.ParseContext) error {
		return consumerLs(ctx, config, c)
	})
	lsCmd.Arg("stream-id", "ID of the stream").StringVar(&c.StreamID)

	addCmd := consumer.Command("add", "Add a new consumer to a stream").Action(func(ctx *fisk.ParseContext) error {
		return consumerAdd(ctx, config, c)
	})
	addCmd.Arg("stream-id", "ID of the stream").StringVar(&c.StreamID)
	addCmd.Arg("slug", "Slug for the new consumer").StringVar(&c.Slug)
	addCmd.Flag("ack-wait-ms", "Acknowledgement wait time in milliseconds").IntVar(&c.AckWaitMS)
	addCmd.Flag("max-ack-pending", "Maximum number of pending acknowledgements").IntVar(&c.MaxAckPending)
	addCmd.Flag("max-deliver", "Maximum number of delivery attempts").IntVar(&c.MaxDeliver)
	addCmd.Flag("max-waiting", "Maximum number of waiting messages").IntVar(&c.MaxWaiting)
	addCmd.Flag("filter-subject-pattern", "Subject pattern for message filtering").StringVar(&c.FilterSubjectPattern)
	addCmd.Flag("defaults", "Use default values for non-required fields").BoolVar(&c.UseDefaults)

	infoCmd := consumer.Command("info", "Show consumer information").Action(func(ctx *fisk.ParseContext) error {
		return consumerInfo(ctx, config, c)
	})
	infoCmd.Arg("stream-id", "ID of the stream").StringVar(&c.StreamID)
	infoCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)

	nextCmd := consumer.Command("next", "Pull next messages for a consumer").Action(func(ctx *fisk.ParseContext) error {
		return consumerNext(ctx, config, c)
	})
	nextCmd.Arg("stream-id", "ID of the stream").StringVar(&c.StreamID)
	nextCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	nextCmd.Flag("batch-size", "Number of messages to fetch").Default("1").IntVar(&c.BatchSize)
	nextCmd.Flag("no-ack", "Do not acknowledge messages").BoolVar(&c.NoAck)

	messagesCmd := consumer.Command("messages", "Show messages for a consumer").Action(func(ctx *fisk.ParseContext) error {
		return consumerMessages(ctx, config, c)
	})
	messagesCmd.Arg("stream-id", "ID of the stream").StringVar(&c.StreamID)
	messagesCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	messagesCmd.Flag("pending", "Show only pending messages").BoolVar(&c.PendingOnly)
	messagesCmd.Flag("last", "Show last N messages").IntVar(&c.LastN)
	messagesCmd.Flag("first", "Show first N messages").IntVar(&c.FirstN)

	ackCmd := consumer.Command("ack", "Acknowledge a message").Action(func(ctx *fisk.ParseContext) error {
		return consumerAck(ctx, config, c)
	})
	ackCmd.Arg("stream-id", "ID of the stream").StringVar(&c.StreamID)
	ackCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	ackCmd.Arg("ack-id", "ID of the message to acknowledge").StringVar(&c.AckID)

	updateCmd := consumer.Command("edit", "Edit an existing consumer").Action(func(ctx *fisk.ParseContext) error {
		return consumerEdit(ctx, config, c)
	})
	updateCmd.Arg("stream-id", "ID of the stream").StringVar(&c.StreamID)
	updateCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	updateCmd.Flag("ack-wait-ms", "Acknowledgement wait time in milliseconds").IntVar(&c.AckWaitMS)
	updateCmd.Flag("max-ack-pending", "Maximum number of pending acknowledgements").IntVar(&c.MaxAckPending)
	updateCmd.Flag("max-deliver", "Maximum number of delivery attempts").IntVar(&c.MaxDeliver)
	updateCmd.Flag("max-waiting", "Maximum number of waiting messages").IntVar(&c.MaxWaiting)
}

func consumerLs(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		fisk.Fatalf("failed to load context: %s", err)
	}

	if c.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		c.StreamID = streamID
	}

	consumers, err := api.FetchConsumers(ctx, c.StreamID)
	if err != nil {
		fisk.Fatalf("failed to fetch consumers: %s", err)
	}

	if len(consumers) == 0 {
		fmt.Println("No consumers found for this stream.")
		return nil
	}

	table := newTableWriter("Consumers")

	table.AddHeaders("ID", "Slug", "Max Ack Pending", "Max Deliver", "Created At")

	for _, consumer := range consumers {
		table.AddRow(
			consumer.ID,
			consumer.Slug,
			strconv.Itoa(consumer.MaxAckPending),
			strconv.Itoa(consumer.MaxDeliver),
			consumer.CreatedAt.Format(time.RFC3339),
		)
	}

	fmt.Print(table.Render())
	fmt.Println()
	return nil
}

func consumerAdd(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return fmt.Errorf("failed to load context: %w", err)
	}

	// Always prompt for required fields
	if c.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		c.StreamID = streamID
	}

	if c.Slug == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter consumer slug:",
			Help:    "A unique identifier for this consumer.",
		}, &c.Slug, survey.WithValidator(survey.Required))
		if err != nil {
			return fmt.Errorf("failed to get user input: %w", err)
		}
	}

	if c.FilterSubjectPattern == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter subject pattern for message filtering:",
			Help:    "A subject pattern to filter which messages this consumer receives. Use '*' as a wildcard.",
		}, &c.FilterSubjectPattern, survey.WithValidator(survey.Required))
		if err != nil {
			return fmt.Errorf("failed to get user input: %w", err)
		}
	}

	// Only prompt for non-required fields if --defaults is not set
	if !c.UseDefaults {
		if c.AckWaitMS == 0 {
			err = promptForInt("Enter acknowledgement wait time in milliseconds (optional):", &c.AckWaitMS)
			if err != nil {
				return err
			}
		}

		if c.MaxAckPending == 0 {
			err = promptForInt("Enter maximum number of pending acknowledgements (optional):", &c.MaxAckPending)
			if err != nil {
				return err
			}
		}

		if c.MaxDeliver == 0 {
			err = promptForInt("Enter maximum number of delivery attempts (optional):", &c.MaxDeliver)
			if err != nil {
				return err
			}
		}

		if c.MaxWaiting == 0 {
			err = promptForInt("Enter maximum number of waiting pull requests (optional):", &c.MaxWaiting)
			if err != nil {
				return err
			}
		}
	}

	createOptions := api.ConsumerCreateOptions{
		Slug:                 c.Slug,
		StreamID:             c.StreamID,
		FilterSubjectPattern: c.FilterSubjectPattern,
	}

	if c.AckWaitMS != 0 {
		createOptions.AckWaitMS = c.AckWaitMS
	}
	if c.MaxAckPending != 0 {
		createOptions.MaxAckPending = c.MaxAckPending
	}
	if c.MaxDeliver != 0 {
		createOptions.MaxDeliver = c.MaxDeliver
	}
	if c.MaxWaiting != 0 {
		createOptions.MaxWaiting = c.MaxWaiting
	}

	consumer, err := api.AddConsumer(ctx, createOptions)
	if err != nil {
		fisk.Fatalf("failed to add consumer: %s", err)
	}

	// Display the created consumer information
	displayConsumerInfo(consumer)

	return nil
}

func displayConsumerInfo(consumer *api.Consumer) {
	fmt.Println()
	cols := newColumns(fmt.Sprintf("Consumer %s created %s", consumer.ID, consumer.CreatedAt.Format(time.RFC3339)))
	cols.AddRow("ID", consumer.ID)
	cols.AddRow("Slug", consumer.Slug)
	cols.AddRow("Stream ID", consumer.StreamID)
	cols.AddRow("Ack Wait (ms)", strconv.Itoa(consumer.AckWaitMS))
	cols.AddRow("Max Ack Pending", strconv.Itoa(consumer.MaxAckPending))
	cols.AddRow("Max Deliver", strconv.Itoa(consumer.MaxDeliver))
	cols.AddRow("Max Waiting", strconv.Itoa(consumer.MaxWaiting))
	cols.AddRow("Filter Subject Pattern", consumer.FilterSubjectPattern)
	cols.AddRow("Created At", consumer.CreatedAt.Format(time.RFC3339))

	cols.Println()

	output, err := cols.Render()
	if err != nil {
		fisk.Fatalf("failed to render columns: %s", err)
	}

	fmt.Print(output)
}

func consumerInfo(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		fisk.Fatalf("failed to load context: %s", err)
	}

	if c.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		c.StreamID = streamID
	}

	if c.ConsumerID == "" {
		consumerID, err := promptForConsumer(ctx, c.StreamID)
		if err != nil {
			return err
		}
		c.ConsumerID = consumerID
	}

	consumer, err := api.FetchConsumerInfo(ctx, c.StreamID, c.ConsumerID)
	if err != nil {
		fisk.Fatalf("failed to fetch consumer info: %s", err)
	}

	displayConsumerInfo(consumer)

	return nil
}

func consumerNext(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		c.StreamID = streamID
	}

	if c.ConsumerID == "" {
		consumerID, err := promptForConsumer(ctx, c.StreamID)
		if err != nil {
			return err
		}
		c.ConsumerID = consumerID
	}

	messages, err := api.FetchNextMessages(ctx, c.StreamID, c.ConsumerID, c.BatchSize)
	if err != nil {
		return err
	}

	if len(messages) == 0 {
		fmt.Println("No messages available.")
		return nil
	}

	for _, msg := range messages {
		fmt.Printf("Message (Ack ID: %s):\n", msg.Info.AckID)
		fmt.Printf("Subject: %s\n", msg.Message.Subject)
		fmt.Printf("Data: %s\n", msg.Message.Data)
		fmt.Printf("Sequence: %d\n", msg.Message.Seq)
		fmt.Printf("Deliver Count: %d\n", msg.Info.DeliverCount)
		fmt.Printf("Last Delivered At: %s\n", msg.Info.LastDeliveredAt)
		fmt.Printf("State: %s\n", msg.Info.State)
		fmt.Println()

		if !c.NoAck {
			err := api.AckMessage(ctx, c.StreamID, c.ConsumerID, msg.Info.AckID)
			if err != nil {
				return fmt.Errorf("failed to acknowledge message: %w", err)
			}
			fmt.Printf("Message acknowledged: %s\n", msg.Info.AckID)
		}
	}

	return nil
}

func consumerMessages(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		c.StreamID = streamID
	}

	if c.ConsumerID == "" {
		consumerID, err := promptForConsumer(ctx, c.StreamID)
		if err != nil {
			return err
		}
		c.ConsumerID = consumerID
	}

	options := api.FetchMessagesOptions{
		StreamID:   c.StreamID,
		ConsumerID: c.ConsumerID,
		Pending:    c.PendingOnly,
		Limit:      10,     // Default limit
		Order:      "desc", // Default order
	}

	if c.LastN > 0 {
		options.Limit = c.LastN
		options.Order = "desc"
	} else if c.FirstN > 0 {
		options.Limit = c.FirstN
		options.Order = "asc"
	}

	messages, err := api.FetchMessages(ctx, options)
	if err != nil {
		return err
	}

	if len(messages) == 0 {
		fmt.Println("No messages found.")
		return nil
	}

	for _, msg := range messages {
		fmt.Printf("Message (Ack ID: %s):\n", msg.Info.AckID)
		fmt.Printf("Subject: %s\n", msg.Message.Subject)
		fmt.Printf("Data: %s\n", msg.Message.Data)
		fmt.Printf("Sequence: %d\n", msg.Message.Seq)
		fmt.Printf("Deliver Count: %d\n", msg.Info.DeliverCount)
		fmt.Printf("Last Delivered At: %s\n", msg.Info.LastDeliveredAt)
		fmt.Printf("State: %s\n", msg.Info.State)
		fmt.Println()
	}

	return nil
}

func consumerAck(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	err = api.AckMessage(ctx, c.StreamID, c.ConsumerID, c.AckID)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	fmt.Printf("Message acknowledged: %s\n", c.AckID)
	return nil
}

func consumerEdit(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	consumer, err := api.FetchConsumerInfo(ctx, c.StreamID, c.ConsumerID)
	if err != nil {
		return fmt.Errorf("could not load Consumer: %w", err)
	}

	// Create a new configuration based on the current one
	newConfig := *consumer // Dereference the pointer to create a copy

	// Update the new configuration with provided values
	if c.AckWaitMS != 0 {
		newConfig.AckWaitMS = c.AckWaitMS
	}
	if c.MaxAckPending != 0 {
		newConfig.MaxAckPending = c.MaxAckPending
	}
	if c.MaxDeliver != 0 {
		newConfig.MaxDeliver = c.MaxDeliver
	}
	if c.MaxWaiting != 0 {
		newConfig.MaxWaiting = c.MaxWaiting
	}

	// Compare the configurations
	diff := cmp.Diff(consumer, &newConfig)
	if diff == "" {
		fmt.Println("No difference in configuration")
		return nil
	}

	fmt.Printf("Differences (-old +new):\n%s", diff)

	if !c.Force {
		ok, err := askConfirmation(fmt.Sprintf("Really edit Consumer %s > %s", c.StreamID, c.ConsumerID), false)
		if err != nil {
			return fmt.Errorf("could not obtain confirmation: %w", err)
		}
		if !ok {
			return nil
		}
	}

	updateOptions := api.ConsumerUpdateOptions{}

	// Update the options with provided values
	if c.AckWaitMS != 0 {
		updateOptions.AckWaitMS = c.AckWaitMS
	}
	if c.MaxAckPending != 0 {
		updateOptions.MaxAckPending = c.MaxAckPending
	}
	if c.MaxDeliver != 0 {
		updateOptions.MaxDeliver = c.MaxDeliver
	}
	if c.MaxWaiting != 0 {
		updateOptions.MaxWaiting = c.MaxWaiting
	}

	updatedConsumer, err := api.EditConsumer(ctx, c.StreamID, c.ConsumerID, updateOptions)
	if err != nil {
		return fmt.Errorf("failed to update consumer: %w", err)
	}

	fmt.Println("Consumer updated successfully:")
	displayConsumerInfo(updatedConsumer)

	return nil
}
