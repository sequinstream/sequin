package cli

import (
	"fmt"

	"github.com/choria-io/fisk"

	"sequin-cli/context"
)

type ctxCommand struct {
	name        string
	description string
	serverURL   string
}

func AddContextCommands(app *fisk.Application, _config *Config) {
	cmd := &ctxCommand{}
	ctx := app.Command("context", "Manage sequin configuration contexts")

	// Add cheats
	addCheat("context", ctx)

	create := ctx.Command("create", "Create or update a context").Action(cmd.createAction)
	create.Arg("name", "The context name").Required().StringVar(&cmd.name)
	create.Flag("description", "Set a friendly description for this context").StringVar(&cmd.description)
	create.Flag("server-url", "The server URL for this context").Required().StringVar(&cmd.serverURL)

	// Add more subcommands like list, show, delete as needed
}

func (c *ctxCommand) createAction(_ *fisk.ParseContext) error {
	ctx := context.Context{
		Name:        c.name,
		Description: c.description,
		ServerURL:   c.serverURL,
	}

	err := context.SaveContext(ctx)
	if err != nil {
		return fmt.Errorf("could not save context: %w", err)
	}

	fmt.Printf("Context '%s' created successfully.\n", c.name)
	return nil
}
