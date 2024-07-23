package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/choria-io/fisk"
	"golang.org/x/term"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
)

type webhookConfig struct {
	Name     string
	StreamID string
}

func addWebhookCommands(webhook *fisk.CmdClause, config *Config) {
	addCheat("webhook", webhook)

	c := &webhookConfig{}

	add := webhook.Command("add", "Add a new webhook").Action(func(_ *fisk.ParseContext) error {
		return webhookAdd(config, c)
	})
	add.Arg("name", "Name of the webhook").StringVar(&c.Name)

	webhook.Command("ls", "List webhooks").Alias("list").Action(func(_ *fisk.ParseContext) error {
		return webhookList(config)
	})

	infoCmd := webhook.Command("info", "Show webhook info").Action(func(_ *fisk.ParseContext) error {
		return webhookInfo(config, c)
	})
	infoCmd.Arg("name", "Name of the webhook to show info for").StringVar(&c.Name)

	rmCmd := webhook.Command("rm", "Remove a webhook").Action(func(_ *fisk.ParseContext) error {
		return webhookRemove(config, &c.Name)
	})
	rmCmd.Arg("name", "Name of the webhook to remove").StringVar(&c.Name)

	// Add the update command
	updateCmd := webhook.Command("update", "Update an existing webhook").Action(func(_ *fisk.ParseContext) error {
		return webhookUpdate(config, c)
	})
	updateCmd.Arg("name", "Name of the webhook to update").StringVar(&c.Name)
	updateCmd.Flag("stream", "New stream ID for the webhook").StringVar(&c.StreamID)
}

func webhookAdd(config *Config, c *webhookConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	// Prompt for stream selection
	c.StreamID, err = promptForStream(ctx)
	if err != nil {
		return err
	}

	if c.Name == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter the name for the new webhook:",
		}, &c.Name, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	webhook := api.WebhookCreate{
		Name:     c.Name,
		StreamID: c.StreamID,
	}

	if config.AsCurl {
		req, err := api.BuildCreateWebhook(ctx, &webhook)
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

	newWebhook, err := api.CreateWebhook(ctx, &webhook)
	if err != nil {
		fmt.Println(err)
		return fmt.Errorf("failed to create webhook")
	}

	fmt.Printf("Webhook created successfully. ID: %s\n", newWebhook.ID)
	err = printWebhookInfo(newWebhook, ctx)
	if err != nil {
		return err
	}

	fmt.Println()

	// Subscribe to webhook channel and wait for first message
	wc, err := api.NewWebhookChannel(ctx)
	if err != nil {
		return fmt.Errorf("failed to create webhook channel: %w", err)
	}

	err = wc.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to webhook channel: %w", err)
	}
	// Create a channel to signal when a message is received
	messageChan := make(chan api.Message)
	wc.OnWebhookIngested(func(webhook api.Webhook, message api.Message) {
		if webhook.ID == newWebhook.ID {
			messageChan <- message
		}
	})

	// Start the loading indicator
	done := make(chan bool)
	go loadingIndicator(done)

	receivedMessage := <-messageChan
	done <- true
	displayWebhookMessage(newWebhook, receivedMessage)

	return nil
}

func loadingIndicator(done chan bool) {
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	i := 0
	for {
		select {
		case <-done:
			return
		default:
			fmt.Printf("\rWaiting for the first webhook message to be processed %s", frames[i])
			time.Sleep(100 * time.Millisecond)
			i = (i + 1) % len(frames)
		}
	}
}

func webhookList(config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return fmt.Errorf("failed to load context: %w", err)
	}

	if config.AsCurl {
		req, err := api.BuildFetchWebhooks(ctx)
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

	webhooks, err := api.FetchWebhooks(ctx)
	if err != nil {
		return err
	}

	if len(webhooks) == 0 {
		fmt.Println("No webhooks defined")
		return nil
	}

	columns := []table.Column{
		{Title: "Name", Width: 20},
		{Title: "Stream", Width: 50},
		{Title: "Created At", Width: 20},
	}

	rows := []table.Row{}
	for _, w := range webhooks {
		rows = append(rows, table.Row{w.Name, w.Stream.Name, w.CreatedAt.Format(time.RFC3339)})
	}

	t := NewTable(columns, rows, PrintableTable)
	return t.Render()
}

func printWebhookInfo(webhook *api.Webhook, ctx *context.Context) error {
	columns := []table.Column{
		{Title: "Field", Width: 20},
		{Title: "Value", Width: 50},
	}

	rows := []table.Row{
		{"Name", webhook.Name},
		{"ID", webhook.ID},
		{"Stream", webhook.Stream.Name},
		{"Account ID", webhook.AccountID},
		{"Created At", webhook.CreatedAt.String()},
		{"Updated At", webhook.UpdatedAt.String()},
		{"Webhook URL", fmt.Sprintf("%s/api/webhook/%s", ctx.ServerURL, webhook.Name)},
	}

	t := NewTable(columns, rows, PrintableTable)
	err := t.Render()
	if err != nil {
		return err
	}

	fmt.Println("\nTo send a webhook payload:")
	fmt.Printf("curl -X POST %s/api/webhook/%s \\\n", ctx.ServerURL, webhook.Name)
	fmt.Println("  -H \"Content-Type: application/json\" \\\n  -d '{\"key\": \"value\"}'")

	return nil
}

func webhookInfo(config *Config, c *webhookConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.Name == "" {
		c.Name, err = promptForWebhook(ctx)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildFetchWebhook(ctx, c.Name)
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

	webhook, err := api.FetchWebhook(ctx, c.Name)
	if err != nil {
		return err
	}

	return printWebhookInfo(webhook, ctx)
}

func webhookRemove(config *Config, name *string) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if *name == "" {
		*name, err = promptForWebhook(ctx)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildDeleteWebhook(ctx, *name)
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

	err = api.DeleteWebhook(ctx, *name)
	if err != nil {
		return err
	}

	fmt.Printf("Webhook %s removed successfully.\n", *name)
	return nil
}

func webhookUpdate(config *Config, c *webhookConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return fmt.Errorf("failed to load context: %w", err)
	}

	if c.Name == "" {
		c.Name, err = promptForWebhook(ctx)
		if err != nil {
			return err
		}
	}

	webhookData := &api.WebhookCreate{
		StreamID: c.StreamID,
	}

	if config.AsCurl {
		req, err := api.BuildUpdateWebhook(ctx, c.Name, webhookData)
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

	updatedWebhook, err := api.UpdateWebhook(ctx, c.Name, webhookData)
	if err != nil {
		return fmt.Errorf("failed to update webhook: %w", err)
	}

	fmt.Printf("Webhook updated successfully. ID: %s\n", updatedWebhook.ID)
	return nil
}

func promptForWebhook(ctx *context.Context) (string, error) {
	webhooks, err := api.FetchWebhooks(ctx)
	if err != nil {
		return "", err
	}

	if len(webhooks) == 0 {
		return "", fmt.Errorf("no webhooks found")
	}

	webhookOptions := make([]string, len(webhooks))
	for i, w := range webhooks {
		webhookOptions[i] = fmt.Sprintf("%s (%s)", w.Name, w.ID)
	}

	var selected string
	err = survey.AskOne(&survey.Select{
		Message: "Choose a webhook:",
		Options: webhookOptions,
		Filter: func(filterValue string, optValue string, index int) bool {
			return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
		},
	}, &selected)
	if err != nil {
		return "", err
	}

	return strings.Split(selected, " ")[0], nil
}

func displayWebhookMessage(webhook *api.Webhook, message api.Message) {
	successStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("green")).
		Bold(true)

	fmt.Println(successStyle.Render("\nWebhook received successfully!"))

	screenWidth, _, _ := term.GetSize(int(os.Stdout.Fd()))
	maxWidth := screenWidth - 4 // Subtract 4 for padding and borders

	// Prepare data for width calculation
	data := []string{
		webhook.Name,
		message.Key,
		time.Now().Format(time.RFC3339),
		message.Data,
	}

	// Calculate max content width
	maxContentWidth := 0
	for _, item := range data {
		if len(item) > maxContentWidth {
			maxContentWidth = len(item)
		}
	}

	// Calculate column widths
	fieldWidth := 20
	valueWidth := min(maxContentWidth, maxWidth-fieldWidth-3)

	columns := []table.Column{
		{Title: "Field", Width: fieldWidth},
		{Title: "Value", Width: valueWidth},
	}

	// Prepare and truncate message data
	truncatedData := truncateMessageData(message.Data, valueWidth, 5)

	rows := []table.Row{
		{"Webhook Name", truncateString(webhook.Name, valueWidth)},
		{"Message Key", truncateString(message.Key, valueWidth)},
		{"Message Created At", truncateString(time.Now().Format(time.RFC3339), valueWidth)},
	}

	// Add message data rows
	for i, line := range truncatedData {
		if i == 0 {
			rows = append(rows, table.Row{"Message Data", line})
		} else {
			rows = append(rows, table.Row{"", line})
		}
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(len(rows)),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(s)

	m := model{t}
	p := tea.NewProgram(m)
	go func() {
		time.Sleep(1 * time.Second)
		p.Quit()
	}()
	if _, err := p.Run(); err != nil {
		fmt.Println("Error running program:", err)
		return
	}
}

// Helper function to truncate and split message data
func truncateMessageData(data string, width, maxLines int) []string {
	lines := strings.Split(data, "\n")
	var result []string
	for i, line := range lines {
		if i >= maxLines {
			break
		}
		result = append(result, truncateString(line, width))
	}
	return result
}

type model struct {
	table table.Model
}

func (m model) Init() tea.Cmd { return nil }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			return m, tea.Quit
		}
	}
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m model) View() string {
	return lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		Render(m.table.View()) + "\n"
}
