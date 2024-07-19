package cli

import (
	"github.com/choria-io/fisk"
)

func AddSourceCommands(app *fisk.Application, config *Config) {
	sources := app.Command("source", "Source related commands")

	// Add postgres subcommand
	postgres := sources.Command("postgres", "Postgres replication related commands").Alias("pg").Alias("p")
	addPostgresReplicationCommands(postgres, config)

	// Add webhook subcommand
	webhook := sources.Command("webhook", "Webhook related commands").Alias("wh")
	addWebhookCommands(webhook, config)
}
