package cli

import (
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"

	"sequin-cli/api"
	"sequin-cli/context"
)

type postgresReplicationConfig struct {
	StreamID        string
	Database        string
	Hostname        string
	Port            int
	Username        string
	Password        string
	SlotName        string
	PublicationName string
	Slug            string
}

func AddPostgresReplicationCommands(app *fisk.Application, config *Config) {
	sources := app.Command("sources", "Source related commands")
	postgres := sources.Command("postgres", "Postgres replication related commands")

	addCheat("postgres_replication", postgres)

	c := &postgresReplicationConfig{}

	add := postgres.Command("add", "Add a new postgres replication").Action(func(ctx *fisk.ParseContext) error {
		return postgresReplicationAdd(ctx, config, c)
	})
	add.Arg("stream", "Stream ID to replicate to").StringVar(&c.StreamID)
	add.Flag("database", "Database name").StringVar(&c.Database)
	add.Flag("hostname", "Database hostname").StringVar(&c.Hostname)
	add.Flag("port", "Database port").IntVar(&c.Port)
	add.Flag("username", "Database username").StringVar(&c.Username)
	add.Flag("password", "Database password").StringVar(&c.Password)
	add.Flag("slot-name", "Replication slot name").StringVar(&c.SlotName)
	add.Flag("publication-name", "Publication name").StringVar(&c.PublicationName)
	add.Flag("slug", "Database slug").StringVar(&c.Slug)

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

func postgresReplicationAdd(_ *fisk.ParseContext, config *Config, c *postgresReplicationConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if c.StreamID == "" {
		streams, err := api.FetchStreams(ctx)
		if err != nil {
			return err
		}

		streamOptions := make([]string, len(streams))
		for i, s := range streams {
			streamOptions[i] = fmt.Sprintf("%s (Index: %d)", s.ID, s.Idx)
		}

		prompt := &survey.Select{
			Message: "Choose a stream:",
			Options: streamOptions,
			Filter: func(filterValue string, optValue string, index int) bool {
				return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
			},
		}

		var choice string
		err = survey.AskOne(prompt, &choice)
		if err != nil {
			return err
		}

		c.StreamID = strings.Split(choice, " ")[0]
	}

	questions := []*survey.Question{
		{
			Name:     "Database",
			Prompt:   &survey.Input{Message: "Enter the dbname for the source database:", Default: "postgres", Help: "A single Postgres server can contain multiple databases. This is the name of the database to replicate from."},
			Validate: survey.Required,
		},
		{
			Name:     "Hostname",
			Prompt:   &survey.Input{Message: "Enter the hostname for the source database:", Default: "localhost", Help: "The hostname of the source database. This is the hostname of the Postgres server."},
			Validate: survey.Required,
		},
		{
			Name:     "Port",
			Prompt:   &survey.Input{Message: "Enter the port for the source database:", Default: "5432", Help: "The port of the source database. This is the port of the Postgres server."},
			Validate: survey.Required,
		},
		{
			Name:     "Username",
			Prompt:   &survey.Input{Message: "Enter the username for the source database:", Default: "postgres", Help: "The username Sequin should use to connect to the source database."},
			Validate: survey.Required,
		},
		{
			Name:     "Password",
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
		{
			Name:     "Slug",
			Prompt:   &survey.Input{Message: "Enter a slug for the database:", Help: "A unique identifier for this database."},
			Validate: survey.Required,
		},
	}

	// Filter out questions that have already been answered via flags
	filteredQuestions := []*survey.Question{}
	for _, q := range questions {
		switch q.Name {
		case "Database":
			if c.Database == "" {
				filteredQuestions = append(filteredQuestions, q)
			}
		case "Hostname":
			if c.Hostname == "" {
				filteredQuestions = append(filteredQuestions, q)
			}
		case "Port":
			if c.Port == 0 {
				filteredQuestions = append(filteredQuestions, q)
			}
		case "Username":
			if c.Username == "" {
				filteredQuestions = append(filteredQuestions, q)
			}
		case "Password":
			if c.Password == "" {
				filteredQuestions = append(filteredQuestions, q)
			}
		case "SlotName":
			if c.SlotName == "" {
				filteredQuestions = append(filteredQuestions, q)
			}
		case "PublicationName":
			if c.PublicationName == "" {
				filteredQuestions = append(filteredQuestions, q)
			}
		case "Slug":
			if c.Slug == "" {
				filteredQuestions = append(filteredQuestions, q)
			}
		}
	}

	err = survey.Ask(filteredQuestions, c)
	if err != nil {
		return err
	}

	// Convert postgresReplicationConfig to api.PostgresReplicationCreate
	replication := api.PostgresReplicationCreate{
		SlotName:        c.SlotName,
		PublicationName: c.PublicationName,
		StreamID:        c.StreamID,
		PostgresDatabase: struct {
			Database string `json:"database"`
			Hostname string `json:"hostname"`
			Port     int    `json:"port"`
			Username string `json:"username"`
			Password string `json:"password"`
			Slug     string `json:"slug"`
		}{
			Database: c.Database,
			Hostname: c.Hostname,
			Port:     c.Port,
			Username: c.Username,
			Password: c.Password,
			Slug:     c.Slug,
		},
	}
	// Create the postgres replication
	newReplication, err := api.CreatePostgresReplication(ctx, &replication)
	fisk.FatalIfError(err, "could not create Postgres replication")

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

	fmt.Println()

	cols := newColumns(fmt.Sprintf("Information for Postgres Replication %s", replication.ID))

	cols.AddRow("ID", replication.ID)
	cols.AddRow("Slot Name", replication.SlotName)
	cols.AddRow("Publication Name", replication.PublicationName)
	cols.AddRow("Status", replication.Status)
	cols.AddRow("Stream ID", replication.StreamID)
	cols.AddRow("Postgres Database ID", replication.PostgresDatabaseID)

	cols.Println()

	output, err := cols.Render()
	if err != nil {
		return err
	}

	fmt.Print(output)

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
