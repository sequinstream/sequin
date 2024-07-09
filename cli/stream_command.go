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

	table := newTableWriter("Streams")

	table.AddHeaders("ID", "Consumers", "Messages", "Created At", "Updated At")
	// Add rows
	for _, s := range streams {
		table.AddRow(
			s.ID,
			s.ConsumerCount,
			s.MessageCount,
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

		streams, err := api.FetchStreams(ctx)
		if err != nil {
			return err
		}

		prompt := &survey.Select{
			Message: "Choose a stream:",
			Options: make([]string, len(streams)),
			Filter: func(filterValue string, optValue string, index int) bool {
				return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
			},
		}
		for i, s := range streams {
			prompt.Options[i] = fmt.Sprintf("%s (Index: %d)", s.ID, s.Idx)
		}

		var choice string
		err = survey.AskOne(prompt, &choice)
		if err != nil {
			return err
		}

		config.StreamID = strings.Split(choice, " ")[0]
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

	// Display stream info
	fmt.Printf("Stream Information:\n")
	fmt.Printf("ID: %s\n", stream.ID)
	fmt.Printf("Index: %d\n", stream.Idx)
	fmt.Printf("Consumers: %d\n", stream.ConsumerCount)
	fmt.Printf("Messages: %d\n", stream.MessageCount)
	fmt.Printf("Created At: %s\n", stream.CreatedAt.Format(time.RFC3339))
	fmt.Printf("Updated At: %s\n", stream.UpdatedAt.Format(time.RFC3339))

	return nil
}
