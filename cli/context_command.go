package cli

import (
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
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

	ctx.Command("ls", "List all contexts").Action(cmd.listAction)

	info := ctx.Command("info", "Show details of a specific context").Action(cmd.infoAction)
	info.Arg("name", "The context name").StringVar(&cmd.name)

	rm := ctx.Command("rm", "Remove a context").Action(cmd.removeAction)
	rm.Arg("name", "The context name").StringVar(&cmd.name)
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

func (c *ctxCommand) listAction(_ *fisk.ParseContext) error {
	contexts, err := context.ListContexts()
	if err != nil {
		return fmt.Errorf("could not list contexts: %w", err)
	}

	if len(contexts) == 0 {
		fmt.Println()
		fmt.Println("No contexts defined")
		fmt.Println()
		return nil
	}

	table := newTableWriter("Contexts")

	table.AddHeaders("Name", "Description", "Server URL")

	for _, ctx := range contexts {
		table.AddRow(
			ctx.Name,
			ctx.Description,
			ctx.ServerURL,
		)
	}

	fmt.Println(table.Render())
	fmt.Println()
	fmt.Println()

	return nil
}

func (c *ctxCommand) infoAction(_ *fisk.ParseContext) error {
	if c.name == "" {
		err := c.pickContext("Choose a context to show info for:")
		if err != nil {
			return err
		}
	}

	ctx, err := context.LoadContext(c.name)
	if err != nil {
		return fmt.Errorf("could not load context: %w", err)
	}

	fmt.Println()

	cols := newColumns(fmt.Sprintf("Information for Context %s", ctx.Name))

	cols.AddRow("Name", ctx.Name)
	cols.AddRow("Description", ctx.Description)
	cols.AddRow("Server URL", ctx.ServerURL)

	cols.Println()

	output, err := cols.Render()
	if err != nil {
		return err
	}

	fmt.Print(output)

	return nil
}

func (c *ctxCommand) removeAction(_ *fisk.ParseContext) error {
	if c.name == "" {
		err := c.pickContext("Choose a context to remove:")
		if err != nil {
			return err
		}
	}

	err := context.RemoveContext(c.name)
	if err != nil {
		return fmt.Errorf("could not remove context: %w", err)
	}

	fmt.Printf("Context '%s' removed successfully.\n", c.name)
	return nil
}

func (c *ctxCommand) pickContext(message string) error {
	contexts, err := context.ListContexts()
	if err != nil {
		return fmt.Errorf("could not list contexts: %w", err)
	}

	if len(contexts) == 0 {
		return fmt.Errorf("no contexts available")
	}

	options := make([]string, len(contexts))
	for i, ctx := range contexts {
		options[i] = fmt.Sprintf("%s (%s)", ctx.Name, ctx.Description)
	}

	prompt := &survey.Select{
		Message: message,
		Options: options,
		Filter: func(filterValue string, optValue string, index int) bool {
			return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
		},
	}

	var choice string
	err = survey.AskOne(prompt, &choice)
	if err != nil {
		return err
	}

	c.name = strings.SplitN(choice, " ", 2)[0]
	return nil
}
