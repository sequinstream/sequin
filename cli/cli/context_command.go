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
	name           string
	hostname       string
	portalHostname string
	tls            bool
	setDefault     bool
	keyID          string
	keySecret      string
}

func AddContextCommands(app *fisk.Application, _config *Config) {
	cmd := &ctxCommand{}
	ctx := app.Command("context", "Manage sequin configuration contexts")

	// Add cheats
	addCheat("context", ctx)

	add := ctx.Command("add", "Add or update a context").Action(cmd.addAction)
	add.Arg("name", "The context name").StringVar(&cmd.name)
	add.Flag("hostname", "The API hostname for this context").
		StringVar(&cmd.hostname)
	add.Flag("portal-hostname", "The Portal hostname for this context").
		StringVar(&cmd.portalHostname)
	add.Flag("tls", "Enable TLS for this context").BoolVar(&cmd.tls)
	add.Flag("set-default", "Set this context as the default").
		BoolVar(&cmd.setDefault)
	add.Flag("key-id", "The Key ID for this context").
		StringVar(&cmd.keyID)
	add.Flag("key-secret", "The Key Secret for this context").
		StringVar(&cmd.keySecret)

	ctx.Command("ls", "List all contexts").Action(cmd.listAction)

	info := ctx.Command("info", "Show details of a specific context").Action(cmd.infoAction)
	info.Arg("name", "The context name").StringVar(&cmd.name)

	rm := ctx.Command("rm", "Remove a context").Action(cmd.removeAction)
	rm.Arg("name", "The context name").StringVar(&cmd.name)

	selectCmd := ctx.Command("select", "Select a default context").Action(cmd.selectAction)
	selectCmd.Arg("name", "The context name").StringVar(&cmd.name)
}

func (c *ctxCommand) addAction(pctx *fisk.ParseContext) error {
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

	if c.keyID == "" {
		prompt := &survey.Input{
			Message: "Enter the Key ID for this context:",
		}
		err := survey.AskOne(prompt, &c.keyID)
		if err != nil {
			return fmt.Errorf("failed to get Key ID: %w", err)
		}
	}

	if c.keySecret == "" {
		prompt := &survey.Password{
			Message: "Enter the Key Secret for this context:",
		}
		err := survey.AskOne(prompt, &c.keySecret)
		if err != nil {
			return fmt.Errorf("failed to get Key Secret: %w", err)
		}
	}

	ctx := context.Context{
		Name:      c.name,
		KeyID:     c.keyID,
		KeySecret: c.keySecret,
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
		{Title: "Hostname", Width: 40},
		{Title: "TLS", Width: 10},
		{Title: "Default", Width: 10},
		{Title: "Key ID", Width: 20},
	}

	rows := []table.Row{}
	for _, ctx := range contexts {
		isDefault := ""
		if ctx.Default {
			isDefault = "✓"
		}
		rows = append(rows, table.Row{
			ctx.Name,
			ctx.Hostname,
			fmt.Sprintf("%t", ctx.TLS),
			isDefault,
			ctx.KeyID,
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
		{"Portal Hostname", ctx.PortalHostname},
		{"Default", fmt.Sprintf("%t", ctx.Default)},
		{"Key ID", ctx.KeyID},
		{"Key Secret", strings.Repeat("*", len(ctx.KeySecret))},
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
		options[i] = ctx.Name
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
				fmt.Println(text.FgYellow.Sprint("There are no contexts available. Would you like to add a new context?"))
				addNew := false
				prompt := &survey.Confirm{
					Message: "Add a new context?",
				}
				survey.AskOne(prompt, &addNew)
				if addNew {
					return c.addAction(nil)
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
