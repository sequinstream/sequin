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

	if len(streams) == 0 {
		fmt.Println()
		fmt.Println("No streams defined")
		fmt.Println()
		return nil
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
			prompt.Options[i] = fmt.Sprintf("%s", s.ID)
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

	fmt.Println()

	cols := newColumns(fmt.Sprintf("Information for Stream %s created %s", stream.ID, stream.CreatedAt.Format(time.RFC3339)))

	cols.AddRow("Index", stream.Idx)
	cols.AddRow("Consumers", stream.ConsumerCount)
	cols.AddRow("Messages", stream.MessageCount)
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
