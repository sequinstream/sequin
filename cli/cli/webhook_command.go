package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
	"github.com/choria-io/fisk"

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
	updateCmd.Flag("stream-id", "New stream ID for the webhook").StringVar(&c.StreamID)
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
	return printWebhookInfo(newWebhook, ctx)
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
