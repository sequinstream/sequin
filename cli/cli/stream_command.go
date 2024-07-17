package cli

import (
	"fmt"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
	"github.com/choria-io/fisk"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
)

type StreamConfig struct {
	Name   string
	Key    string
	Data   string
	Filter string
	Last   int
	First  int
	Table  bool
}

func AddStreamCommands(app *fisk.Application, config *Config) {
	stream := app.Command("stream", "Stream related commands").Alias("str").Alias("s")

	addCheat("stream", stream)

	s := &StreamConfig{}

	stream.Command("ls", "List streams").Action(func(c *fisk.ParseContext) error {
		return streamLs(c, config)
	})

	infoCmd := stream.Command("info", "Show stream info").Action(func(c *fisk.ParseContext) error {
		return streamInfo(c, config)
	})
	infoCmd.Arg("stream-id", "ID or name of the stream to show info for").StringVar(&config.StreamID)

	addCmd := stream.Command("add", "Add a new stream").Action(func(c *fisk.ParseContext) error {
		return streamAdd(c, config, s)
	})
	addCmd.Arg("name", "Name of the stream to Add").StringVar(&s.Name)

	rmCmd := stream.Command("rm", "Remove a stream").Action(func(c *fisk.ParseContext) error {
		return streamRm(c, config)
	})
	rmCmd.Arg("stream-id", "ID or name of the stream to remove").StringVar(&config.StreamID)

	sendCmd := stream.Command("send", "Send a message to a stream").Action(func(c *fisk.ParseContext) error {
		return streamSend(c, config, s)
	})
	sendCmd.Arg("stream-id", "ID or name of the stream to send to").Required().StringVar(&config.StreamID)
	sendCmd.Arg("key", "Key of the message").Required().StringVar(&s.Key)
	sendCmd.Arg("data", "Data payload of the message").Required().StringVar(&s.Data)

	viewCmd := stream.Command("view", "View messages in a stream").Action(func(c *fisk.ParseContext) error {
		return streamView(c, config, s)
	})
	viewCmd.Arg("stream-id", "ID or name of the stream to view").StringVar(&config.StreamID)
	viewCmd.Flag("filter", "Filter messages by key pattern").StringVar(&s.Filter)
	viewCmd.Flag("last", "Show most recent N messages").Default("10").IntVar(&s.Last)
	viewCmd.Flag("first", "Show least recent N messages").IntVar(&s.First)
	viewCmd.Flag("table", "Display messages in a table format").BoolVar(&s.Table)

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
		{Title: "Name", Width: 20},
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
			s.Name,
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
	fmt.Printf("Information for Stream %s created %s\n\n", stream.Name, stream.CreatedAt.Format(time.RFC3339))
	return t.Render()
}

func streamAdd(_ *fisk.ParseContext, config *Config, s *StreamConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	// Prompt for name if not provided
	if s.Name == "" {
		prompt := &survey.Input{
			Message: "Enter stream name:",
		}
		err = survey.AskOne(prompt, &s.Name)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildAddStream(ctx, s.Name)
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
	stream, err := api.AddStream(ctx, s.Name)
	if err != nil {
		return fmt.Errorf("failed to add stream: %w", err)
	}

	columns := []table.Column{
		{Title: "Field", Width: 20},
		{Title: "Value", Width: 50},
	}

	rows := []table.Row{
		{"Name", stream.Name},
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

func streamSend(_ *fisk.ParseContext, config *Config, s *StreamConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildPublishMessage(ctx, config.StreamID, s.Key, s.Data)
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

	err = api.PublishMessage(ctx, config.StreamID, s.Key, s.Data)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	fmt.Printf("Message sent to stream '%s' with key '%s'\n", config.StreamID, s.Key)
	return nil
}

func streamView(_ *fisk.ParseContext, config *Config, s *StreamConfig) error {
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

	limit := s.Last
	sort := "seq_desc"

	if s.First > 0 {
		limit = s.First
		sort = "seq_asc"
	}

	populator := func() ([]api.Message, error) {
		return api.ListStreamMessages(ctx, config.StreamID, limit, sort, s.Filter)
	}

	messages, err := populator()
	if err != nil {
		return fmt.Errorf("failed to list messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Println("No messages found")
		return nil
	}

	if s.Table {
		return displayMessagesAsTable(messages, populator)
	}

	return displayMessagesAsDump(messages)
}
