package cli

import (
	"fmt"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"

	"sequin-cli/api"
	"sequin-cli/context"
)

type StreamAddConfig struct {
	Slug string
}

var streamAddConfig StreamAddConfig

type StreamPublishConfig struct {
	StreamSlug string
	Subject    string
	Message    string
}

var streamPublishConfig StreamPublishConfig

type StreamListConfig struct {
	StreamIDOrSlug string
	Last           int
	First          int
	SubjectPattern string
}

var streamListConfig StreamListConfig

// AddStreamCommands adds all stream-related commands to the given app
func AddStreamCommands(app *fisk.Application, config *Config) {
	stream := app.Command("stream", "Stream related commands").Alias("str").Alias("s")

	addCheat("stream", stream)

	stream.Command("ls", "List streams").Action(func(c *fisk.ParseContext) error {
		return streamLs(c, config)
	})

	infoCmd := stream.Command("info", "Show stream info").Action(func(c *fisk.ParseContext) error {
		return streamInfo(c, config)
	})
	infoCmd.Arg("stream-id", "ID of the stream to show info for").StringVar(&config.StreamID)

	addCmd := stream.Command("add", "Add a new stream").Action(func(c *fisk.ParseContext) error {
		return streamAdd(c, config)
	})
	addCmd.Arg("slug", "Slug of the stream to Add").StringVar(&streamAddConfig.Slug)

	rmCmd := stream.Command("rm", "Remove a stream").Action(func(c *fisk.ParseContext) error {
		return streamRm(c, config)
	})
	rmCmd.Arg("stream-id", "ID of the stream to remove").StringVar(&config.StreamID)

	pubCmd := stream.Command("pub", "Publish a message to a stream").Action(func(c *fisk.ParseContext) error {
		return streamPublish(c, config)
	})
	pubCmd.Arg("stream-id-or-slug", "ID or slug of the stream to publish to").Required().StringVar(&streamPublishConfig.StreamSlug)
	pubCmd.Arg("subject", "Subject of the message").Required().StringVar(&streamPublishConfig.Subject)
	pubCmd.Arg("message", "Message to publish").Required().StringVar(&streamPublishConfig.Message)

	messagesCmd := stream.Command("messages", "List messages in a stream").Action(func(c *fisk.ParseContext) error {
		return streamList(c, config)
	})
	messagesCmd.Arg("stream-id-or-slug", "ID or slug of the stream").Required().StringVar(&streamListConfig.StreamIDOrSlug)
	messagesCmd.Flag("last", "Show last N messages").Default("10").IntVar(&streamListConfig.Last)
	messagesCmd.Flag("first", "Show first N messages").IntVar(&streamListConfig.First)
	messagesCmd.Flag("subject", "Filter messages by subject pattern").StringVar(&streamListConfig.SubjectPattern)
}

func streamLs(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildFetchStreams(ctx)
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

	streams, err := api.FetchStreams(ctx)
	if err != nil {
		return err
	}

	if len(streams) == 0 {
		fmt.Println("No streams defined")
		return nil
	}

	table := newTableWriter("Streams")

	table.AddHeaders("ID", "Slug", "Consumers", "Messages", "Storage Size", "Created At", "Updated At")
	// Add rows
	for _, s := range streams {
		table.AddRow(
			s.ID,
			s.Slug,
			s.Stats.ConsumerCount,
			s.Stats.MessageCount,
			formatBytes(s.Stats.StorageSize),
			s.CreatedAt.Format(time.RFC3339),
			s.UpdatedAt.Format(time.RFC3339),
		)
	}

	// Render the table
	fmt.Print(table.Render())

	return nil
}

func streamInfo(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		config.StreamID = streamID
	}

	if config.AsCurl {
		req, err := api.BuildFetchStreamInfo(ctx, config.StreamID)
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

	return displayStreamInfo(config)
}

func displayStreamInfo(config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	stream, err := api.FetchStreamInfo(ctx, config.StreamID)
	if err != nil {
		return err
	}

	cols := newColumns(fmt.Sprintf("Information for Stream %s created %s", stream.Slug, stream.CreatedAt.Format(time.RFC3339)))

	cols.AddRow("ID", stream.ID)
	cols.AddRow("Index", stream.Idx)
	cols.AddRow("Consumers", stream.Stats.ConsumerCount)
	cols.AddRow("Messages", stream.Stats.MessageCount)
	cols.AddRow("Storage Size", formatBytes(stream.Stats.StorageSize))
	cols.AddRow("Created At", stream.CreatedAt.Format(time.RFC3339))
	cols.AddRow("Updated At", stream.UpdatedAt.Format(time.RFC3339))

	cols.Println()

	output, err := cols.Render()
	if err != nil {
		return err
	}

	fmt.Print(output)

	return nil
}

func streamAdd(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	// Prompt for slug if not provided
	if streamAddConfig.Slug == "" {
		prompt := &survey.Input{
			Message: "Enter stream slug:",
		}
		err = survey.AskOne(prompt, &streamAddConfig.Slug)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildAddStream(ctx, streamAddConfig.Slug)
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

	// Add stream
	stream, err := api.AddStream(ctx, streamAddConfig.Slug)
	if err != nil {
		return fmt.Errorf("failed to add stream: %w", err)
	}

	cols := newColumns(fmt.Sprintf("Stream %s created %s", stream.ID, stream.CreatedAt.Format(time.RFC3339)))

	cols.AddRow("Slug", stream.Slug)
	cols.AddRow("Index", stream.Idx)
	cols.AddRow("Consumers", stream.Stats.ConsumerCount)
	cols.AddRow("Messages", stream.Stats.MessageCount)
	cols.AddRow("Storage Size", formatBytes(stream.Stats.StorageSize))
	cols.AddRow("Created At", stream.CreatedAt.Format(time.RFC3339))
	cols.AddRow("Updated At", stream.UpdatedAt.Format(time.RFC3339))

	cols.Println()

	output, err := cols.Render()
	if err != nil {
		return err
	}

	fmt.Print(output)

	return nil
}

func streamRm(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		config.StreamID = streamID
	}

	if config.AsCurl {
		req, err := api.BuildRemoveStream(ctx, config.StreamID)
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

	err = api.RemoveStream(ctx, config.StreamID)
	if err != nil {
		return fmt.Errorf("failed to remove stream: %w", err)
	}

	fmt.Printf("Stream %s has been removed.\n", config.StreamID)
	return nil
}

func streamPublish(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildPublishMessage(ctx, streamPublishConfig.StreamSlug, streamPublishConfig.Subject, streamPublishConfig.Message)
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

	// Pass the streamSlug directly to the PublishMessage function
	err = api.PublishMessage(ctx, streamPublishConfig.StreamSlug, streamPublishConfig.Subject, streamPublishConfig.Message)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	fmt.Printf("Message published to stream '%s' with subject '%s'\n", streamPublishConfig.StreamSlug, streamPublishConfig.Subject)
	return nil
}

func streamList(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	limit := streamListConfig.Last
	sort := "seq_desc"

	if streamListConfig.First > 0 {
		limit = streamListConfig.First
		sort = "seq_asc"
	}

	if config.AsCurl {
		req, err := api.BuildListStreamMessages(ctx, streamListConfig.StreamIDOrSlug, limit, sort, streamListConfig.SubjectPattern)
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

	messages, err := api.ListStreamMessages(ctx, streamListConfig.StreamIDOrSlug, limit, sort, streamListConfig.SubjectPattern)
	if err != nil {
		return fmt.Errorf("failed to list messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Printf("No messages found in stream '%s'\n", streamListConfig.StreamIDOrSlug)
		return nil
	}

	fmt.Printf("Listing %d messages in stream %s\n", len(messages), streamListConfig.StreamIDOrSlug)

	for _, msg := range messages {
		fmt.Println()
		fmt.Println()
		fmt.Printf("Sequence:   %d\n", msg.Seq)
		fmt.Printf("Subject:    %s\n", msg.Subject)
		fmt.Printf("Created At: %s\n", msg.CreatedAt.Format(time.RFC3339))
		fmt.Printf("Updated At: %s\n", msg.UpdatedAt.Format(time.RFC3339))
		fmt.Println()
		fmt.Println(msg.Data)
	}

	return nil
}
