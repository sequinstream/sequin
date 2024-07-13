package cli

import (
	"fmt"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
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
	Key        string
	Message    string
}

var streamPublishConfig StreamPublishConfig

type StreamListConfig struct {
	StreamIDOrSlug string
	Last           int
	First          int
	KeyPattern     string
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
	pubCmd.Arg("key", "Key of the message").Required().StringVar(&streamPublishConfig.Key)
	pubCmd.Arg("message", "Message to publish").Required().StringVar(&streamPublishConfig.Message)

	messagesCmd := stream.Command("messages", "List messages in a stream").Action(func(c *fisk.ParseContext) error {
		return streamList(c, config)
	})
	messagesCmd.Arg("stream-id-or-slug", "ID or slug of the stream").Required().StringVar(&streamListConfig.StreamIDOrSlug)
	messagesCmd.Flag("last", "Show most recent N messages").Default("10").IntVar(&streamListConfig.Last)
	messagesCmd.Flag("first", "Show least recent N messages").IntVar(&streamListConfig.First)
	messagesCmd.Flag("filter", "Filter messages by key pattern").StringVar(&streamListConfig.KeyPattern)
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

	columns := []table.Column{
		{Title: "ID", Width: 36},
		{Title: "Slug", Width: 20},
		{Title: "Consumers", Width: 10},
		{Title: "Messages", Width: 10},
		{Title: "Storage Size", Width: 15},
		{Title: "Created At", Width: 30},
		{Title: "Updated At", Width: 30},
	}

	rows := []table.Row{}
	for _, s := range streams {
		rows = append(rows, table.Row{
			s.ID,
			s.Slug,
			fmt.Sprintf("%d", s.Stats.ConsumerCount),
			fmt.Sprintf("%d", s.Stats.MessageCount),
			formatBytes(s.Stats.StorageSize),
			s.CreatedAt.Format(time.RFC3339),
			s.UpdatedAt.Format(time.RFC3339),
		})
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Println("Streams")
	return t.Render()
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

	columns := []table.Column{
		{Title: "Field", Width: 20},
		{Title: "Value", Width: 50},
	}

	rows := []table.Row{
		{"ID", stream.ID},
		{"Index", fmt.Sprintf("%d", stream.Idx)},
		{"Consumers", fmt.Sprintf("%d", stream.Stats.ConsumerCount)},
		{"Messages", fmt.Sprintf("%d", stream.Stats.MessageCount)},
		{"Storage Size", formatBytes(stream.Stats.StorageSize)},
		{"Created At", stream.CreatedAt.Format(time.RFC3339)},
		{"Updated At", stream.UpdatedAt.Format(time.RFC3339)},
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Printf("Information for Stream %s created %s\n\n", stream.Slug, stream.CreatedAt.Format(time.RFC3339))
	return t.Render()
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

	columns := []table.Column{
		{Title: "Field", Width: 20},
		{Title: "Value", Width: 50},
	}

	rows := []table.Row{
		{"Slug", stream.Slug},
		{"Index", fmt.Sprintf("%d", stream.Idx)},
		{"Consumers", fmt.Sprintf("%d", stream.Stats.ConsumerCount)},
		{"Messages", fmt.Sprintf("%d", stream.Stats.MessageCount)},
		{"Storage Size", formatBytes(stream.Stats.StorageSize)},
		{"Created At", stream.CreatedAt.Format(time.RFC3339)},
		{"Updated At", stream.UpdatedAt.Format(time.RFC3339)},
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Printf("Stream %s created %s\n\n", stream.ID, stream.CreatedAt.Format(time.RFC3339))
	return t.Render()
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
		req, err := api.BuildPublishMessage(ctx, streamPublishConfig.StreamSlug, streamPublishConfig.Key, streamPublishConfig.Message)
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
	err = api.PublishMessage(ctx, streamPublishConfig.StreamSlug, streamPublishConfig.Key, streamPublishConfig.Message)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	fmt.Printf("Message published to stream '%s' with key '%s'\n", streamPublishConfig.StreamSlug, streamPublishConfig.Key)
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
		req, err := api.BuildListStreamMessages(ctx, streamListConfig.StreamIDOrSlug, limit, sort, streamListConfig.KeyPattern)
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

	messages, err := api.ListStreamMessages(ctx, streamListConfig.StreamIDOrSlug, limit, sort, streamListConfig.KeyPattern)
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
		fmt.Printf("Key:    %s\n", msg.Key)
		fmt.Printf("Created At: %s\n", msg.CreatedAt.Format(time.RFC3339))
		fmt.Printf("Updated At: %s\n", msg.UpdatedAt.Format(time.RFC3339))
		fmt.Println()
		fmt.Println(msg.Data)
	}

	return nil
}
