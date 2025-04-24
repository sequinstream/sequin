package cli

import (
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
	"github.com/choria-io/fisk"
	"github.com/google/go-cmp/cmp"
	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/sequinstream/sequin/cli/context"
)

type ctxCommand struct {
	name          string
	hostname      string
	portalBaseURL string
	tls           bool
	tlsProvided   bool  // Track whether --tls flag was explicitly provided
	setDefault    bool
	apiToken      string
	tunnelPorts   string // New field for tunnel ports
	force         bool   // New field for force edit
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
	add.Flag("portal-base-url", "The Portal hostname for this context").
		StringVar(&cmd.portalBaseURL)
	add.Flag("tls", "Enable TLS for this context").
		IsSetByUser(&cmd.tlsProvided).
		BoolVar(&cmd.tls)
	add.Flag("set-default", "Set this context as the default").
		BoolVar(&cmd.setDefault)
	add.Flag("api-token", "The API Token for this context").
		StringVar(&cmd.apiToken)
	add.Flag("tunnel-ports", "Comma-separated list of tunnel ports in the format port:nameOrId").
		StringVar(&cmd.tunnelPorts)

	ctx.Command("ls", "List all contexts").Action(cmd.listAction)

	info := ctx.Command("info", "Show details of a specific context").Action(cmd.infoAction)
	info.Alias("show")
	info.Arg("name", "The context name").StringVar(&cmd.name)

	rm := ctx.Command("rm", "Remove a context").Action(cmd.removeAction)
	rm.Arg("name", "The context name").StringVar(&cmd.name)

	selectCmd := ctx.Command("select", "Select a default context").Action(cmd.selectAction)
	selectCmd.Arg("name", "The context name").StringVar(&cmd.name)

	edit := ctx.Command("edit", "Edit an existing context").Action(cmd.editAction)
	edit.Arg("name", "The context name to edit").StringVar(&cmd.name)
	edit.Flag("hostname", "The API hostname for this context").StringVar(&cmd.hostname)
	edit.Flag("portal-base-url", "The Portal hostname for this context").StringVar(&cmd.portalBaseURL)
	edit.Flag("tls", "Enable TLS for this context").
		IsSetByUser(&cmd.tlsProvided).
		BoolVar(&cmd.tls)
	edit.Flag("api-token", "The API Token for this context").StringVar(&cmd.apiToken)
	edit.Flag("tunnel-ports", "Comma-separated list of tunnel ports in the format port:nameOrId").StringVar(&cmd.tunnelPorts)
	edit.Flag("force", "Force edit without confirmation").BoolVar(&cmd.force)
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

	if c.apiToken == "" {
		prompt := &survey.Password{
			Message: "Enter the API Token for this context:",
		}
		err := survey.AskOne(prompt, &c.apiToken)
		if err != nil {
			return fmt.Errorf("failed to get API Token: %w", err)
		}
	}

	// Decide the default *only* if --tls was not on the CLI
	if !c.tlsProvided {
		if isLocalHostname(c.hostname) {
			c.tls = false          // local ⇒ plain-HTTP
		} else {
			c.tls = true           // remote ⇒ HTTPS
		}
	}

	ctx := context.Context{
		Name:          c.name,
		ApiToken:      c.apiToken,
		Hostname:      c.hostname,
		TLS:           c.tls,
		PortalBaseURL: c.portalBaseURL,
	}

	// Parse and add tunnel ports if provided
	if c.tunnelPorts != "" {
		tunnelPorts, err := parseTunnelPorts(c.tunnelPorts)
		if err != nil {
			return fmt.Errorf("failed to parse tunnel ports: %w", err)
		}
		ctx.TunnelPorts = tunnelPorts
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

// Add a helper function to check if a hostname is local
func isLocalHostname(hostname string) bool {
	hostname = strings.ToLower(hostname)
	hostname = strings.Split(hostname, ":")[0] // Remove port if present
	
	return hostname == "localhost" ||
		   hostname == "127.0.0.1" ||
		   hostname == "::1" ||
		   hostname == "host.docker.internal" ||
		   strings.HasPrefix(hostname, "192.168.") ||
		   strings.HasPrefix(hostname, "10.") ||
		   strings.HasPrefix(hostname, "172.16.")
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
		{"Portal Base URL", ctx.PortalBaseURL},
		{"Default", fmt.Sprintf("%t", ctx.Default)},
		{"API Token", strings.Repeat("*", len(ctx.ApiToken))},
	}

	// Add tunnel ports information
	if len(ctx.TunnelPorts) > 0 {
		tunnelPortsStr := formatTunnelPorts(ctx.TunnelPorts)
		rows = append(rows, table.Row{"Tunnel Ports", tunnelPortsStr})
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

	c.name = choice
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

// New helper function to parse tunnel ports
func parseTunnelPorts(input string) ([]map[string]string, error) {
	var tunnelPorts []map[string]string
	pairs := strings.Split(input, ",")
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid tunnel port format: %s", pair)
		}
		tunnelPorts = append(tunnelPorts, map[string]string{
			"port":     parts[0],
			"nameOrId": parts[1],
		})
	}
	return tunnelPorts, nil
}

// New helper function to format tunnel ports for display
func formatTunnelPorts(tunnelPorts []map[string]string) string {
	var formatted []string
	for _, tp := range tunnelPorts {
		formatted = append(formatted, fmt.Sprintf("%s:%s", tp["port"], tp["nameOrId"]))
	}
	return strings.Join(formatted, ", ")
}

func (c *ctxCommand) editAction(_ *fisk.ParseContext) error {
	if c.name == "" {
		err := c.pickContext("Choose a context to edit:")
		if err != nil {
			return err
		}
	}

	existingCtx, err := context.LoadContext(c.name)
	if err != nil {
		return fmt.Errorf("failed to load context: %w", err)
	}

	// Create a new context based on the existing one
	newCtx := *existingCtx

	// Update the new context with provided values
	if c.hostname != "" {
		newCtx.Hostname = c.hostname
	}
	if c.portalBaseURL != "" {
		newCtx.PortalBaseURL = c.portalBaseURL
	}
	if c.tlsProvided {            // user asked to change it
		newCtx.TLS = c.tls
	}
	if c.apiToken != "" {
		newCtx.ApiToken = c.apiToken
	}
	if c.tunnelPorts != "" {
		tunnelPorts, err := parseTunnelPorts(c.tunnelPorts)
		if err != nil {
			return fmt.Errorf("failed to parse tunnel ports: %w", err)
		}
		newCtx.TunnelPorts = tunnelPorts
	}

	// Compare the configurations
	diff := cmp.Diff(existingCtx, &newCtx)
	if diff == "" {
		fmt.Println("No changes to apply.")
		return nil
	}

	fmt.Printf("Differences (-old +new):\n%s", diff)

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really edit context %s", c.name), false)
		if err != nil {
			return fmt.Errorf("could not obtain confirmation: %w", err)
		}
		if !ok {
			return nil
		}
	}

	err = context.SaveContext(newCtx)
	if err != nil {
		return fmt.Errorf("failed to save updated context: %w", err)
	}

	fmt.Printf("Context '%s' updated successfully.\n", c.name)
	return nil
}
