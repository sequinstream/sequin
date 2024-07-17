package cli

import (
	"fmt"
	"strconv"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
	"github.com/choria-io/fisk"
	"github.com/google/go-cmp/cmp"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
)

type consumerConfig struct {
	StreamID         string
	ConsumerID       string
	Slug             string
	AckWaitMS        int
	MaxAckPending    int
	MaxDeliver       int
	MaxWaiting       int
	FilterKeyPattern string
	BatchSize        int
	NoAck            bool
	PendingOnly      bool
	LastN            int
	FirstN           int
	AckId            string
	Force            bool
	UseDefaults      bool
	Kind             string
	URL              string
	BearerToken      string
}

func AddConsumerCommands(app *fisk.Application, config *Config) {
	consumer := app.Command("consumer", "Consumer related commands").Alias("con").Alias("c")

	addCheat("consumer", consumer)
	c := &consumerConfig{}

	lsCmd := consumer.Command("ls", "List consumers").Action(func(ctx *fisk.ParseContext) error {
		return consumerLs(ctx, config, c)
	})
	lsCmd.Arg("stream-id", "ID or slug of the stream").StringVar(&c.StreamID)

	addCmd := consumer.Command("add", "Add a new consumer").Action(func(ctx *fisk.ParseContext) error {
		return consumerAdd(ctx, config, c)
	})
	addCmd.Arg("stream-id", "ID or slug of the stream").StringVar(&c.StreamID)
	addCmd.Arg("slug", "Slug for the new consumer").StringVar(&c.Slug)
	addCmd.Flag("ack-wait-ms", "Acknowledgement wait time in milliseconds").IntVar(&c.AckWaitMS)
	addCmd.Flag("max-ack-pending", "Maximum number of pending acknowledgements").IntVar(&c.MaxAckPending)
	addCmd.Flag("max-deliver", "Maximum number of delivery attempts").IntVar(&c.MaxDeliver)
	addCmd.Flag("max-waiting", "Maximum number of waiting messages").IntVar(&c.MaxWaiting)
	addCmd.Flag("filter", "Key pattern for message filtering").StringVar(&c.FilterKeyPattern)
	addCmd.Flag("defaults", "Use default values for non-required fields").BoolVar(&c.UseDefaults)
	addCmd.Flag("kind", "Consumer kind (pull or push)").StringVar(&c.Kind)
	addCmd.Flag("url", "URL for push consumer").StringVar(&c.URL)
	addCmd.Flag("bearer-token", "Bearer token for push consumer").StringVar(&c.BearerToken)

	infoCmd := consumer.Command("info", "Show consumer information").Action(func(ctx *fisk.ParseContext) error {
		return consumerInfo(ctx, config, c)
	})
	infoCmd.Arg("stream-id", "ID or slug of the stream").StringVar(&c.StreamID)
	infoCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)

	receiveCmd := consumer.Command("receive", "Receive messages for a consumer").Action(func(ctx *fisk.ParseContext) error {
		return consumerReceive(ctx, config, c)
	})
	receiveCmd.Arg("stream-id", "ID or slug of the stream").StringVar(&c.StreamID)
	receiveCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	receiveCmd.Flag("batch-size", "Number of messages to fetch").Default("1").IntVar(&c.BatchSize)
	receiveCmd.Flag("no-ack", "Do not acknowledge messages").BoolVar(&c.NoAck)

	peekCmd := consumer.Command("peek", "Show messages for a consumer").Action(func(ctx *fisk.ParseContext) error {
		return consumerPeek(ctx, config, c)
	})
	peekCmd.Arg("stream-id", "ID or slug of the stream").StringVar(&c.StreamID)
	peekCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	peekCmd.Flag("pending", "Show only pending messages").BoolVar(&c.PendingOnly)
	peekCmd.Flag("last", "Show most recent N messages").IntVar(&c.LastN)
	peekCmd.Flag("first", "Show least recent N messages").IntVar(&c.FirstN)

	ackCmd := consumer.Command("ack", "Ack a message").Action(func(ctx *fisk.ParseContext) error {
		return consumerAck(ctx, config, c)
	})
	ackCmd.Arg("stream-id", "ID or slug of the stream").StringVar(&c.StreamID)
	ackCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	ackCmd.Arg("ack-token", "Ack token of the message to ack").StringVar(&c.AckId)

	nackCmd := consumer.Command("nack", "Nack a message").Action(func(ctx *fisk.ParseContext) error {
		return consumerNack(ctx, config, c)
	})
	nackCmd.Arg("stream-id", "ID or slug of the stream").StringVar(&c.StreamID)
	nackCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	nackCmd.Arg("ack-id", "ID of the message to nack").StringVar(&c.AckId)

	updateCmd := consumer.Command("edit", "Edit an existing consumer").Action(func(ctx *fisk.ParseContext) error {
		return consumerEdit(ctx, config, c)
	})
	updateCmd.Arg("stream-id", "ID or slug of the stream").StringVar(&c.StreamID)
	updateCmd.Arg("consumer-id", "ID of the consumer").StringVar(&c.ConsumerID)
	updateCmd.Flag("ack-wait-ms", "Acknowledgement wait time in milliseconds").IntVar(&c.AckWaitMS)
	updateCmd.Flag("max-ack-pending", "Maximum number of pending acknowledgements").IntVar(&c.MaxAckPending)
	updateCmd.Flag("max-deliver", "Maximum number of delivery attempts").IntVar(&c.MaxDeliver)
	updateCmd.Flag("max-waiting", "Maximum number of waiting messages").IntVar(&c.MaxWaiting)

	rmCmd := consumer.Command("rm", "Remove a consumer").Action(func(ctx *fisk.ParseContext) error {
		return consumerRemove(ctx, config, c)
	})
	rmCmd.Arg("stream-id", "ID or slug of the stream").Required().StringVar(&c.StreamID)
	rmCmd.Arg("consumer-id", "ID of the consumer to remove").StringVar(&c.ConsumerID)
	rmCmd.Flag("force", "Force removal without confirmation").BoolVar(&c.Force)
}

// Helper function to get the first available stream
func getFirstStream(ctx *context.Context) (string, error) {
	streams, err := api.FetchStreams(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch streams: %w", err)
	}
	if len(streams) == 0 {
		return "", fmt.Errorf("no streams available")
	}
	return streams[0].ID, nil
}

func consumerLs(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return fmt.Errorf("failed to load context: %w", err)
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildFetchConsumers(ctx, c.StreamID)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)

		return nil
	}

	consumers, err := api.FetchConsumers(ctx, c.StreamID)
	if err != nil {
		return fmt.Errorf("failed to fetch consumers: %w", err)
	}

	if len(consumers) == 0 {
		fmt.Println("No consumers found for this stream.")
		return nil
	}

	columns := []table.Column{
		{Title: "ID", Width: 36},
		{Title: "Slug", Width: 20},
		{Title: "Max Ack Pending", Width: 15},
		{Title: "Max Deliver", Width: 12},
		{Title: "Created At", Width: 30},
	}

	rows := []table.Row{}
	for _, consumer := range consumers {
		rows = append(rows, table.Row{
			consumer.ID,
			consumer.Slug,
			strconv.Itoa(consumer.MaxAckPending),
			strconv.Itoa(consumer.MaxDeliver),
			consumer.CreatedAt.Format(time.RFC3339),
		})
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Println("Consumers")
	return t.Render()
}

func consumerAdd(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return fmt.Errorf("failed to load context: %w", err)
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
	}

	// Always prompt for required fields
	if c.Slug == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter consumer slug:",
			Help:    "A unique identifier for this consumer.",
		}, &c.Slug, survey.WithValidator(survey.Required))
		if err != nil {
			return fmt.Errorf("failed to get user input: %w", err)
		}
	}

	if c.FilterKeyPattern == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter key pattern for message filtering:",
			Help:    "A key pattern to filter which messages this consumer receives. Use '*' as a wildcard.",
		}, &c.FilterKeyPattern, survey.WithValidator(survey.Required))
		if err != nil {
			return fmt.Errorf("failed to get user input: %w", err)
		}
	}

	if c.Kind == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Select consumer kind:",
			Options: []string{"pull", "push"},
			Default: "pull",
		}, &c.Kind)
		if err != nil {
			return fmt.Errorf("failed to get user input: %w", err)
		}
	}

	var httpEndpoint *api.HttpEndpointOptions

	if c.Kind == "push" {
		if c.URL == "" {
			err = survey.AskOne(&survey.Input{
				Message: "Enter URL to send messages to:",
				Help:    "The URL where messages will be pushed to.",
			}, &c.URL, survey.WithValidator(survey.Required))
			if err != nil {
				return fmt.Errorf("failed to get user input: %w", err)
			}
		}

		if c.BearerToken == "" {
			err = survey.AskOne(&survey.Input{
				Message: "Enter desired Bearer token to use in HTTP requests (optional):",
				Help:    "The Bearer token to use in requests to the push URL. Set this if you want to have a way to authenticate inbound requests from Sequin (recommended for production).",
			}, &c.BearerToken)
			if err != nil {
				return fmt.Errorf("failed to get user input: %w", err)
			}
		}

		httpEndpoint = &api.HttpEndpointOptions{
			BaseURL: c.URL,
			Headers: map[string]string{
				"Authorization": "Bearer " + c.BearerToken,
			},
		}
	} else if c.Kind == "pull" {
		// Only prompt for these fields if it's a pull consumer and --defaults is not set
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

			if c.MaxWaiting == 0 {
				err = promptForInt("Enter maximum number of waiting pull requests (optional):", &c.MaxWaiting)
				if err != nil {
					return err
				}
			}
		}
	}

	createOptions := api.ConsumerCreateOptions{
		Slug:             c.Slug,
		StreamID:         c.StreamID,
		FilterKeyPattern: c.FilterKeyPattern,
		Kind:             c.Kind,
		HttpEndpoint:     httpEndpoint,
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

	if config.AsCurl {
		req, err := api.BuildAddConsumer(ctx, createOptions)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)

		return nil
	}

	consumer, err := api.AddConsumer(ctx, createOptions)
	if err != nil {
		switch e := err.(type) {
		case *api.ValidationError:
			fmt.Println("Validation failed:")
			e.PrintValidationError()
		case *api.APIError:
			e.PrintAPIError()
		default:
			return fmt.Errorf("failed to add consumer: %w", err)
		}
		return err
	}

	// Display the created consumer information
	displayConsumerInfo(consumer)

	return nil
}

func displayConsumerInfo(consumer *api.Consumer) {
	columns := []table.Column{
		{Title: "Field", Width: 20},
		{Title: "Value", Width: 50},
	}

	rows := []table.Row{
		{"ID", consumer.ID},
		{"Slug", consumer.Slug},
		{"Stream ID", consumer.StreamID},
		{"Ack Wait (ms)", strconv.Itoa(consumer.AckWaitMS)},
		{"Max Ack Pending", strconv.Itoa(consumer.MaxAckPending)},
		{"Max Deliver", strconv.Itoa(consumer.MaxDeliver)},
		{"Max Waiting", strconv.Itoa(consumer.MaxWaiting)},
		{"Filter", consumer.FilterKeyPattern},
		{"Created At", consumer.CreatedAt.Format(time.RFC3339)},
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Printf("Consumer %s created %s\n\n", consumer.ID, consumer.CreatedAt.Format(time.RFC3339))
	t.Render()
}

func consumerInfo(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return fmt.Errorf("failed to load context: %w", err)
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
	}

	if c.ConsumerID == "" {
		c.ConsumerID, err = promptForConsumer(ctx, c.StreamID)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildFetchConsumerInfo(ctx, c.StreamID, c.ConsumerID)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)

		return nil
	}

	consumer, err := api.FetchConsumerInfo(ctx, c.StreamID, c.ConsumerID)
	if err != nil {
		return fmt.Errorf("failed to fetch consumer info: %w", err)
	}

	displayConsumerInfo(consumer)

	return nil
}

func consumerReceive(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
	}

	if c.ConsumerID == "" {
		c.ConsumerID, err = promptForConsumer(ctx, c.StreamID)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildFetchNextMessages(ctx, c.StreamID, c.ConsumerID, c.BatchSize)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)

		return nil
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
		fmt.Printf("Message (Ack Token: %s):\n", msg.AckId)
		fmt.Printf("Key: %s\n", msg.Message.Key)
		fmt.Printf("Sequence: %d\n", msg.Message.Seq)
		fmt.Printf("\n%s\n", msg.Message.Data)

		if !c.NoAck {
			err := api.AckMessage(ctx, c.StreamID, c.ConsumerID, msg.AckId)
			if err != nil {
				return fmt.Errorf("failed to acknowledge message: %w", err)
			}
			fmt.Printf("Message acknowledged with token %s\n", msg.AckId)
		}
	}

	return nil
}

func consumerPeek(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
	}

	if c.ConsumerID == "" {
		c.ConsumerID, err = promptForConsumer(ctx, c.StreamID)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		options := api.FetchMessagesOptions{
			StreamID:   c.StreamID,
			ConsumerID: c.ConsumerID,
			Visible:    !c.PendingOnly, // Invert PendingOnly to get Visible
			Limit:      10,             // Default limit
			Order:      "seq_desc",     // Default order
		}

		if c.LastN > 0 {
			options.Limit = c.LastN
			options.Order = "seq_desc"
		} else if c.FirstN > 0 {
			options.Limit = c.FirstN
			options.Order = "seq_asc"
		}

		req, err := api.BuildFetchMessages(ctx, options)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)

		return nil
	}

	options := api.FetchMessagesOptions{
		StreamID:   c.StreamID,
		ConsumerID: c.ConsumerID,
		Visible:    !c.PendingOnly, // Invert PendingOnly to get Visible
		Limit:      10,             // Default limit
		Order:      "seq_desc",     // Default order
	}

	if c.LastN > 0 {
		options.Limit = c.LastN
		options.Order = "seq_desc"
	} else if c.FirstN > 0 {
		options.Limit = c.FirstN
		options.Order = "seq_asc"
	}

	messages, err := api.FetchMessages(ctx, options)
	if err != nil {
		return fmt.Errorf("failed to fetch messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Println("No messages found.")
		return nil
	}

	columns := []table.Column{
		{Title: "Key", Width: 20},
		{Title: "Sequence", Width: 10},
		{Title: "Deliver Count", Width: 15},
		{Title: "Last Delivered At", Width: 30},
		{Title: "Not Visible Until", Width: 30},
		{Title: "State", Width: 10},
		{Title: "Data", Width: 50},
	}

	rows := []table.Row{}
	for _, msg := range messages {
		rows = append(rows, table.Row{
			msg.Message.Key,
			strconv.Itoa(msg.Message.Seq),
			strconv.Itoa(msg.Info.DeliverCount),
			msg.Info.FormatLastDeliveredAt(),
			msg.Info.FormatNotVisibleUntil(),
			msg.Info.State,
			msg.Message.Data,
		})
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Println("Messages")
	return t.Render()
}

func consumerAck(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildAckMessage(ctx, c.StreamID, c.ConsumerID, c.AckId)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)

		return nil
	}

	err = api.AckMessage(ctx, c.StreamID, c.ConsumerID, c.AckId)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	fmt.Printf("Message acknowledged with token %s\n", c.AckId)
	return nil
}

func consumerNack(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildNackMessage(ctx, c.StreamID, c.ConsumerID, c.AckId)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)

		return nil
	}

	err = api.NackMessage(ctx, c.StreamID, c.ConsumerID, c.AckId)
	if err != nil {
		return fmt.Errorf("failed to nack message: %w", err)
	}

	fmt.Printf("Message nacked: %s\n", c.AckId)
	return nil
}

func consumerEdit(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
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

	if config.AsCurl {
		req, err := api.BuildEditConsumer(ctx, c.StreamID, c.ConsumerID, updateOptions)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)

		return nil
	}

	updatedConsumer, err := api.EditConsumer(ctx, c.StreamID, c.ConsumerID, updateOptions)
	if err != nil {
		return fmt.Errorf("failed to update consumer: %w", err)
	}

	fmt.Println("Consumer updated successfully:")
	displayConsumerInfo(updatedConsumer)

	return nil
}

func consumerRemove(_ *fisk.ParseContext, config *Config, c *consumerConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		c.StreamID, err = promptForStream(ctx)
		if err != nil {
			return err
		}
	}

	if c.ConsumerID == "" {
		c.ConsumerID, err = promptForConsumer(ctx, c.StreamID)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildRemoveConsumer(ctx, c.StreamID, c.ConsumerID)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)
		return nil
	}

	if !c.Force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove Consumer %s > %s", c.StreamID, c.ConsumerID), false)
		if err != nil {
			return fmt.Errorf("could not obtain confirmation: %w", err)
		}
		if !ok {
			return nil
		}
	}

	err = api.RemoveConsumer(ctx, c.StreamID, c.ConsumerID)
	if err != nil {
		return fmt.Errorf("failed to remove consumer: %w", err)
	}

	fmt.Printf("Consumer %s removed successfully\n", c.ConsumerID)
	return nil
}
