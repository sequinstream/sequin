package cli

import (
	"fmt"
	"strings"
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
	Limit          int
	Sort           string
	SubjectPattern string
}

var streamListConfig StreamListConfig

// AddStreamCommands adds all stream-related commands to the given app
func AddStreamCommands(app *fisk.Application, config *Config) {
	stream := app.Command("stream", "Stream related commands")

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
	addCmd.Arg("slug", "Slug of the stream to Add").Required().StringVar(&streamAddConfig.Slug)

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

	lastCmd := stream.Command("last", "List last messages in a stream").Action(func(c *fisk.ParseContext) error {
		streamListConfig.Sort = "seq_desc"
		return streamList(c, config, "last")
	})
	lastCmd.Arg("stream-id-or-slug", "ID or slug of the stream").Required().StringVar(&streamListConfig.StreamIDOrSlug)
	lastCmd.Flag("limit", "Limit the number of messages").Default("10").IntVar(&streamListConfig.Limit)
	lastCmd.Flag("subject", "Filter messages by subject pattern").StringVar(&streamListConfig.SubjectPattern)

	firstCmd := stream.Command("first", "List first messages in a stream").Action(func(c *fisk.ParseContext) error {
		streamListConfig.Sort = "seq_asc"
		return streamList(c, config, "first")
	})
	firstCmd.Arg("stream-id-or-slug", "ID or slug of the stream").Required().StringVar(&streamListConfig.StreamIDOrSlug)
	firstCmd.Flag("limit", "Limit the number of messages").Default("10").IntVar(&streamListConfig.Limit)
	firstCmd.Flag("subject", "Filter messages by subject pattern").StringVar(&streamListConfig.SubjectPattern)
}

func streamLs(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	streams, err := api.FetchStreams(ctx)
	if err != nil {
		return err
	}

	if len(streams) == 0 {
		fmt.Println()
		fmt.Println("No streams defined")
		fmt.Println()
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
	// Print a couple of empty lines after the table
	fmt.Println()
	fmt.Println()

	return nil
}

func streamInfo(_ *fisk.ParseContext, config *Config) error {
	if config.StreamID == "" {
		ctx, err := context.LoadContext(config.ContextName)
		if err != nil {
			return err
		}

		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		config.StreamID = streamID
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

	fmt.Println()

	cols := newColumns(fmt.Sprintf("Information for Stream %s created %s", stream.ID, stream.CreatedAt.Format(time.RFC3339)))

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

	// Add stream
	stream, err := api.AddStream(ctx, streamAddConfig.Slug)
	if err != nil {
		return fmt.Errorf("failed to add stream: %w", err)
	}

	fmt.Println()

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

	// Pass the streamSlug directly to the PublishMessage function
	err = api.PublishMessage(ctx, streamPublishConfig.StreamSlug, streamPublishConfig.Subject, streamPublishConfig.Message)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	fmt.Printf("Message published to stream '%s' with subject '%s'\n", streamPublishConfig.StreamSlug, streamPublishConfig.Subject)
	return nil
}

func streamList(_ *fisk.ParseContext, config *Config, listType string) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	messages, err := api.ListStreamMessages(ctx, streamListConfig.StreamIDOrSlug, streamListConfig.Limit, streamListConfig.Sort, streamListConfig.SubjectPattern)
	if err != nil {
		return fmt.Errorf("failed to list messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Printf("No messages found in stream '%s'\n", streamListConfig.StreamIDOrSlug)
		return nil
	}

	table := newTableWriter(fmt.Sprintf("%s %d Messages in Stream %s", strings.Title(listType), len(messages), streamListConfig.StreamIDOrSlug))
	table.AddHeaders("Sequence", "Subject", "Data", "Created At", "Updated At")

	for _, msg := range messages {
		table.AddRow(
			msg.Seq,
			msg.Subject,
			truncateString(msg.Data, 50),
			msg.CreatedAt.Format(time.RFC3339),
			msg.UpdatedAt.Format(time.RFC3339),
		)
	}

	fmt.Print(table.Render())
	fmt.Println()

	return nil
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
