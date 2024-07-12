package cli

import (
	"fmt"
	"time"

	"github.com/choria-io/fisk"

	"sequin-cli/api"
	"sequin-cli/context"
)

type MessageConfig struct {
	Key    string
	Data   string
	Filter string
	Last   int
	First  int
}

func AddMessageCommands(app *fisk.Application, config *Config) {
	message := app.Command("message", "Message related commands").Alias("msg").Alias("m")

	addCheat("message", message)

	m := &MessageConfig{}

	upsertCmd := message.Command("upsert", "Upsert a message").Action(func(ctx *fisk.ParseContext) error {
		return messageUpsert(ctx, config, m)
	})
	upsertCmd.Arg("key", "Key of the message").Required().StringVar(&m.Key)
	upsertCmd.Arg("data", "Data payload of the message").Required().StringVar(&m.Data)

	message.Command("info", "Show message info").Action(func(ctx *fisk.ParseContext) error {
		return messageInfo(ctx, config)
	})

	listCmd := message.Command("list", "List messages").Alias("ls").Action(func(ctx *fisk.ParseContext) error {
		return messageList(ctx, config, m)
	})
	listCmd.Flag("filter", "Filter messages by key pattern").StringVar(&m.Filter)
	listCmd.Flag("last", "Show most recent N messages").Default("10").IntVar(&m.Last)
	listCmd.Flag("first", "Show least recent N messages").IntVar(&m.First)
}

func messageUpsert(_ *fisk.ParseContext, config *Config, m *MessageConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	streamID, err := getFirstStream(ctx)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildPublishMessage(ctx, streamID, m.Key, m.Data)
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

	err = api.PublishMessage(ctx, streamID, m.Key, m.Data)
	if err != nil {
		return fmt.Errorf("failed to upsert message: %w", err)
	}

	fmt.Printf("Message upserted with key '%s'\n", m.Key)
	return nil
}

func messageInfo(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	streamID, err := getFirstStream(ctx)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildFetchStreamInfo(ctx, streamID)
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

	stream, err := api.FetchStreamInfo(ctx, streamID)
	if err != nil {
		return err
	}

	fmt.Printf("Total messages: %d\n", stream.Stats.MessageCount)
	fmt.Printf("Storage size: %s\n", formatBytes(stream.Stats.StorageSize))
	return nil
}

func messageList(_ *fisk.ParseContext, config *Config, m *MessageConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	streamID, err := getFirstStream(ctx)
	if err != nil {
		return err
	}

	limit := m.Last
	sort := "seq_desc"

	if m.First > 0 {
		limit = m.First
		sort = "seq_asc"
	}

	if config.AsCurl {
		req, err := api.BuildListStreamMessages(ctx, streamID, limit, sort, m.Filter)
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

	messages, err := api.ListStreamMessages(ctx, streamID, limit, sort, m.Filter)
	if err != nil {
		return fmt.Errorf("failed to list messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Println("No messages found")
		return nil
	}

	fmt.Printf("Listing %d messages\n", len(messages))

	for _, msg := range messages {
		fmt.Println()
		fmt.Printf("Key:        %s\n", msg.Key)
		fmt.Printf("Sequence:   %d\n", msg.Seq)
		fmt.Printf("Created At: %s\n", msg.CreatedAt.Format(time.RFC3339))
		fmt.Printf("Updated At: %s\n", msg.UpdatedAt.Format(time.RFC3339))
		fmt.Println("Data:")
		fmt.Println(msg.Data)
	}

	return nil
}
