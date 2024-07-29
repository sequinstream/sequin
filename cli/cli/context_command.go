package cli

import (
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
	"github.com/choria-io/fisk"
	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/sequinstream/sequin/cli/context"
)

type ctxCommand struct {
	name        string
	description string
	hostname    string
	tls         bool
	setDefault  bool
}

func AddContextCommands(app *fisk.Application, _config *Config) {
	cmd := &ctxCommand{}
	ctx := app.Command("context", "Manage sequin configuration contexts")

	// Add cheats
	addCheat("context", ctx)

	create := ctx.Command("create", "Create or update a context").Action(cmd.createAction)
	create.Arg("name", "The context name").StringVar(&cmd.name)
	create.Flag("description", "Set a friendly description for this context").StringVar(&cmd.description)
	create.Flag("hostname", "The hostname for this context").StringVar(&cmd.hostname)
	create.Flag("tls", "Enable TLS for this context").BoolVar(&cmd.tls)
	create.Flag("set-default", "Set this context as the default").BoolVar(&cmd.setDefault)

	ctx.Command("ls", "List all contexts").Action(cmd.listAction)

	info := ctx.Command("info", "Show details of a specific context").Action(cmd.infoAction)
	info.Arg("name", "The context name").StringVar(&cmd.name)

	rm := ctx.Command("rm", "Remove a context").Action(cmd.removeAction)
	rm.Arg("name", "The context name").StringVar(&cmd.name)

	selectCmd := ctx.Command("select", "Select a default context").Action(cmd.selectAction)
	selectCmd.Arg("name", "The context name").StringVar(&cmd.name)
}

func (c *ctxCommand) createAction(pctx *fisk.ParseContext) error {
	// Use pre-set values if available, otherwise prompt
	if c.name == "" {
		prompt := &survey.Input{
			Message: "Enter the context name:",
		}
		err := survey.AskOne(prompt, &c.name)
		if err != nil {
			return fmt.Errorf("failed to get context name: %w", err)
		}
	}

	if c.description == "" {
		prompt := &survey.Input{
			Message: "Enter a description for the context (optional):",
		}
		err := survey.AskOne(prompt, &c.description)
		if err != nil {
			return fmt.Errorf("failed to get context description: %w", err)
		}
	}

	if c.hostname == "" {
		prompt := &survey.Input{
			Message: "Enter the hostname (e.g., localhost:7376 or sequin.io):",
		}
		err := survey.AskOne(prompt, &c.hostname)
		if err != nil {
			return fmt.Errorf("failed to get hostname: %w", err)
		}
	}

	if !c.tls {
		prompt := &survey.Confirm{
			Message: "Enable TLS for this context?",
			Default: false,
		}
		err := survey.AskOne(prompt, &c.tls)
		if err != nil {
			return fmt.Errorf("failed to get TLS option: %w", err)
		}
	}

	ctx := context.Context{
		Name:        c.name,
		Description: c.description,
		Hostname:    c.hostname,
		TLS:         c.tls,
	}

	err := context.SaveContext(ctx)
	if err != nil {
		return fmt.Errorf("could not save context: %w", err)
	}

	fmt.Print(text.FgGreen.Sprintf("Context '%s' created successfully.\n", c.name))

	if !c.setDefault {
		setAsDefault := false
		prompt := &survey.Confirm{
			Message: fmt.Sprintf("Do you want to set '%s' as the default context?", c.name),
		}
		err := survey.AskOne(prompt, &setAsDefault)
		if err != nil {
			return fmt.Errorf("failed to get user input: %w", err)
		}
		c.setDefault = setAsDefault
	}

	if c.setDefault {
		err := context.SetDefaultContext(c.name)
		if err != nil {
			return fmt.Errorf("could not set default context: %w", err)
		}
		fmt.Printf("Context '%s' has been set as the default.\n", c.name)
	}

	return nil
}

func (c *ctxCommand) listAction(_ *fisk.ParseContext) error {
	contexts, err := context.ListContexts()
	if err != nil {
		return fmt.Errorf("could not list contexts: %w", err)
	}

	if len(contexts) == 0 {
		fmt.Println(text.FgBlue.Sprint("No contexts defined"))
		return nil
	}

	columns := []table.Column{
		{Title: "Name", Width: 20},
		{Title: "Description", Width: 30},
		{Title: "Hostname", Width: 40},
		{Title: "TLS", Width: 10},
		{Title: "Default", Width: 10},
	}

	rows := []table.Row{}
	for _, ctx := range contexts {
		isDefault := ""
		if ctx.Default {
			isDefault = "✓"
		}
		rows = append(rows, table.Row{
			ctx.Name,
			ctx.Description,
			ctx.Hostname,
			fmt.Sprintf("%t", ctx.TLS),
			isDefault,
		})
	}
	t := NewTable(columns, rows, PrintableTable)
	fmt.Println("Contexts")
	return t.Render()
}

func (c *ctxCommand) infoAction(_ *fisk.ParseContext) error {
	if c.name == "" {
		err := c.pickContext("Choose a context to show info for:")
		if err != nil {
			if err.Error() == "no contexts available" {
				fmt.Println(text.FgBlue.Sprint("There are no contexts available."))
				return nil
			}
			return err
		}
	}

	ctx, err := context.LoadContext(c.name)
	if err != nil {
		return fmt.Errorf("could not load context: %w", err)
	}

	columns := []table.Column{
		{Title: "Property", Width: 20},
		{Title: "Value", Width: 40},
	}

	rows := []table.Row{
		{"Name", ctx.Name},
		{"Description", ctx.Description},
		{"Hostname", ctx.Hostname},
		{"TLS", fmt.Sprintf("%t", ctx.TLS)},
		{"Default", fmt.Sprintf("%t", ctx.Default)},
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Printf("Information for Context %s\n", ctx.Name)
	return t.Render()
}

func (c *ctxCommand) removeAction(_ *fisk.ParseContext) error {
	if c.name == "" {
		err := c.pickContext("Choose a context to remove:")
		if err != nil {
			if err.Error() == "no contexts available" {
				fmt.Println(text.FgBlue.Sprint("There are no contexts available to delete."))
				return nil
			}
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
		if ctx.Description != "" {
			options[i] = fmt.Sprintf("%s (%s)", ctx.Name, ctx.Description)
		} else {
			options[i] = ctx.Name
		}
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

func (c *ctxCommand) selectAction(_ *fisk.ParseContext) error {
	if c.name == "" {
		err := c.pickContext("Choose a context to set as default:")
		if err != nil {
			if err.Error() == "no contexts available" {
				fmt.Println(text.FgYellow.Sprint("There are no contexts available. Would you like to create a new context?"))
				createNew := false
				prompt := &survey.Confirm{
					Message: "Create a new context?",
				}
				survey.AskOne(prompt, &createNew)
				if createNew {
					return c.createAction(nil)
				}
				return nil
			}
			return err
		}
	}

	ctx, err := context.LoadContext(c.name)
	if err != nil {
		return fmt.Errorf("could not load context '%s': %w", c.name, err)
	}

	err = context.SetDefaultContext(ctx.Name)
	if err != nil {
		return fmt.Errorf("could not set default context: %w", err)
	}

	fmt.Printf("Default context set to '%s'.\n", ctx.Name)
	return nil
}
