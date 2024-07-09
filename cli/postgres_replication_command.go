package cli

import (
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"

	"sequin-cli/api"
	"sequin-cli/context"
)

func AddPostgresReplicationCommands(app *fisk.Application, config *Config) {
	sources := app.Command("sources", "Source related commands")
	postgres := sources.Command("postgres", "Postgres replication related commands")

	addCheat("postgres_replication", postgres)

	postgres.Command("add", "Add a new postgres replication").Action(func(c *fisk.ParseContext) error {
		return postgresReplicationAdd(c, config)
	})

	postgres.Command("ls", "List postgres replications").Action(func(c *fisk.ParseContext) error {
		return postgresReplicationList(c, config)
	})

	infoCmd := postgres.Command("info", "Show postgres replication info").Action(func(c *fisk.ParseContext) error {
		return postgresReplicationInfo(c, config)
	})
	infoCmd.Arg("id", "ID of the postgres replication to show info for").StringVar(&config.PostgresReplicationID)

	rmCmd := postgres.Command("rm", "Remove a postgres replication").Action(func(c *fisk.ParseContext) error {
		return postgresReplicationRemove(c, config)
	})
	rmCmd.Arg("id", "ID of the postgres replication to remove").StringVar(&config.PostgresReplicationID)
}

func postgresReplicationAdd(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	// Prompt for required information
	var replication api.PostgresReplicationCreate

	// Fetch available streams
	streams, err := api.FetchStreams(ctx)
	if err != nil {
		return err
	}

	// Prepare stream options
	streamOptions := make([]string, len(streams))
	for i, s := range streams {
		streamOptions[i] = fmt.Sprintf("%s (Index: %d)", s.ID, s.Idx)
	}

	err = survey.Ask([]*survey.Question{
		{
			Name: "StreamID",
			Prompt: &survey.Select{
				Message: "Choose a stream:",
				Options: streamOptions,
				Filter: func(filterValue string, optValue string, index int) bool {
					return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
				},
			},
			Validate: survey.Required,
		},
		{
			Name:     "PostgresDatabase.Database",
			Prompt:   &survey.Input{Message: "Enter the dbname for the source database:", Default: "postgres", Help: "A single Postgres server can contain multiple databases. This is the name of the database to replicate from."},
			Validate: survey.Required,
		},
		{
			Name:     "PostgresDatabase.Hostname",
			Prompt:   &survey.Input{Message: "Enter the hostname for the source database:", Default: "localhost", Help: "The hostname of the source database. This is the hostname of the Postgres server."},
			Validate: survey.Required,
		},
		{
			Name:     "PostgresDatabase.Port",
			Prompt:   &survey.Input{Message: "Enter the port for the source database:", Default: "5432", Help: "The port of the source database. This is the port of the Postgres server."},
			Validate: survey.Required,
		},
		{
			Name:     "PostgresDatabase.Username",
			Prompt:   &survey.Input{Message: "Enter the username for the source database:", Default: "postgres", Help: "The username Sequin should use to connect to the source database."},
			Validate: survey.Required,
		},
		{
			Name:     "PostgresDatabase.Password",
			Prompt:   &survey.Password{Message: "Enter the password for the source database:"},
			Validate: survey.Required,
		},
		{
			Name:     "SlotName",
			Prompt:   &survey.Input{Message: "Enter the replication slot's name:", Default: "sequin_replication_slot", Help: "The name of the replication slot you configured for Sequin to replicate from."},
			Validate: survey.Required,
		},
		{
			Name:     "PublicationName",
			Prompt:   &survey.Input{Message: "Enter the publication name:", Default: "sequin_replication_publication", Help: "The name of the publication you configured for Sequin to replicate from."},
			Validate: survey.Required,
		},
	}, &replication)

	if err != nil {
		return err
	}

	// Extract the stream ID from the selected option
	replication.StreamID = strings.Split(replication.StreamID, " ")[0]

	// Create the postgres replication
	newReplication, err := api.CreatePostgresReplication(ctx, &replication)
	if err != nil {
		return err
	}

	fmt.Printf("Postgres replication created successfully. ID: %s\n", newReplication.ID)
	return nil
}

func postgresReplicationList(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	replications, err := api.FetchPostgresReplications(ctx)
	if err != nil {
		return err
	}

	table := newTableWriter("Postgres Replications")

	table.AddHeaders("ID", "Slot Name", "Publication Name", "Status", "Stream ID")
	for _, r := range replications {
		table.AddRow(r.ID, r.SlotName, r.PublicationName, r.Status, r.StreamID)
	}

	fmt.Print(table.Render())
	fmt.Println()
	fmt.Println()

	return nil
}

func postgresReplicationInfo(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.PostgresReplicationID == "" {
		replications, err := api.FetchPostgresReplications(ctx)
		if err != nil {
			return err
		}

		prompt := &survey.Select{
			Message: "Choose a postgres replication:",
			Options: make([]string, len(replications)),
			Filter: func(filterValue string, optValue string, index int) bool {
				return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
			},
		}
		for i, r := range replications {
			prompt.Options[i] = fmt.Sprintf("%s (Slot: %s, Publication: %s)", r.ID, r.SlotName, r.PublicationName)
		}

		var choice string
		err = survey.AskOne(prompt, &choice)
		if err != nil {
			return err
		}

		config.PostgresReplicationID = strings.Split(choice, " ")[0]
	}

	replication, err := api.FetchPostgresReplicationInfo(ctx, config.PostgresReplicationID)
	if err != nil {
		return err
	}

	fmt.Printf("ID: %s\n", replication.ID)
	fmt.Printf("Slot Name: %s\n", replication.SlotName)
	fmt.Printf("Publication Name: %s\n", replication.PublicationName)
	fmt.Printf("Status: %s\n", replication.Status)
	fmt.Printf("Stream ID: %s\n", replication.StreamID)
	fmt.Printf("Postgres Database ID: %s\n", replication.PostgresDatabaseID)

	return nil
}
func postgresReplicationRemove(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.PostgresReplicationID == "" {
		replications, err := api.FetchPostgresReplications(ctx)
		if err != nil {
			return err
		}

		prompt := &survey.Select{
			Message: "Choose a postgres replication to remove:",
			Options: make([]string, len(replications)),
			Filter: func(filterValue string, optValue string, index int) bool {
				return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
			},
		}
		for i, r := range replications {
			prompt.Options[i] = fmt.Sprintf("%s (Slot: %s, Publication: %s)", r.ID, r.SlotName, r.PublicationName)
		}

		var choice string
		err = survey.AskOne(prompt, &choice)
		if err != nil {
			return err
		}

		config.PostgresReplicationID = strings.Split(choice, " ")[0]
	}

	err = api.DeletePostgresReplication(ctx, config.PostgresReplicationID)
	if err != nil {
		return err
	}

	fmt.Printf("Postgres replication %s removed successfully.\n", config.PostgresReplicationID)
	return nil
}
