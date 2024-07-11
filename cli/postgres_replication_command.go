package cli

import (
	"fmt"
	"strconv"
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
	postgres := sources.Command("postgres", "Postgres replication related commands").Alias("pg").Alias("p")

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
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		c.StreamID = streamID
	}

	if c.Database == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter the dbname for the source database:",
			Default: "postgres",
			Help:    "A single Postgres server can contain multiple databases. This is the name of the database to replicate from.",
		}, &c.Database, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if c.Hostname == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter the hostname for the source database:",
			Default: "localhost",
			Help:    "The hostname of the source database. This is the hostname of the Postgres server.",
		}, &c.Hostname, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if c.Port == 0 {
		var portStr string
		err = survey.AskOne(&survey.Input{
			Message: "Enter the port for the source database:",
			Default: "5432",
			Help:    "The port of the source database. This is the port of the Postgres server.",
		}, &portStr, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
		c.Port, err = strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("invalid port number: %v", err)
		}
	}

	if c.Username == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter the username for the source database:",
			Default: "postgres",
			Help:    "The username Sequin should use to connect to the source database.",
		}, &c.Username, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if c.Password == "" {
		err = survey.AskOne(&survey.Password{
			Message: "Enter the password for the source database:",
		}, &c.Password, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if c.SlotName == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter the replication slot's name:",
			Default: "sequin_replication_slot",
			Help:    "The name of the replication slot you configured for Sequin to replicate from.",
		}, &c.SlotName, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if c.PublicationName == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter the publication name:",
			Default: "sequin_replication_publication",
			Help:    "The name of the publication you configured for Sequin to replicate from.",
		}, &c.PublicationName, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if c.Slug == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Enter a slug for the database:",
			Help:    "A unique identifier for this database.",
		}, &c.Slug, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
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
	newReplication, err := api.AddPostgresReplication(ctx, &replication)
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

	if len(replications) == 0 {
		fmt.Println()
		fmt.Println("No sources defined")
		fmt.Println()
		return nil
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

		replicationOptions := make([]string, len(replications))
		for i, r := range replications {
			replicationOptions[i] = fmt.Sprintf("%s (Slot: %s, Publication: %s)", r.ID, r.SlotName, r.PublicationName)
		}

		err = survey.AskOne(&survey.Select{
			Message: "Choose a postgres replication:",
			Options: replicationOptions,
			Filter: func(filterValue string, optValue string, index int) bool {
				return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
			},
		}, &config.PostgresReplicationID)
		if err != nil {
			return err
		}
		config.PostgresReplicationID = strings.Split(config.PostgresReplicationID, " ")[0]
	}

	replicationWithInfo, err := api.FetchPostgresReplicationInfo(ctx, config.PostgresReplicationID)
	if err != nil {
		return err
	}

	fmt.Println()

	cols := newColumns(fmt.Sprintf("Information for Postgres Replication %s", replicationWithInfo.PostgresReplication.ID))

	cols.AddRow("ID", replicationWithInfo.PostgresReplication.ID)
	cols.AddRow("Slot Name", replicationWithInfo.PostgresReplication.SlotName)
	cols.AddRow("Publication Name", replicationWithInfo.PostgresReplication.PublicationName)
	cols.AddRow("Status", replicationWithInfo.PostgresReplication.Status)
	cols.AddRow("Stream ID", replicationWithInfo.PostgresReplication.StreamID)
	cols.AddRow("Postgres Database ID", replicationWithInfo.PostgresReplication.PostgresDatabaseID)

	// Add the new info fields
	lastCommittedTS := replicationWithInfo.Info.FormatLastCommittedAt()
	cols.AddRow("Last Committed Timestamp", lastCommittedTS)

	lagInBytes := "N/A"
	if replicationWithInfo.Info.LagInBytes != nil {
		lagInBytes = fmt.Sprintf("%d", *replicationWithInfo.Info.LagInBytes)
	}
	cols.AddRow("Lag in Bytes", lagInBytes)

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

		replicationOptions := make([]string, len(replications))
		for i, r := range replications {
			replicationOptions[i] = fmt.Sprintf("%s (Slot: %s, Publication: %s)", r.ID, r.SlotName, r.PublicationName)
		}

		err = survey.AskOne(&survey.Select{
			Message: "Choose a postgres replication to remove:",
			Options: replicationOptions,
			Filter: func(filterValue string, optValue string, index int) bool {
				return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
			},
		}, &config.PostgresReplicationID)
		if err != nil {
			return err
		}
		config.PostgresReplicationID = strings.Split(config.PostgresReplicationID, " ")[0]
	}

	err = api.DeletePostgresReplication(ctx, config.PostgresReplicationID)
	if err != nil {
		return err
	}

	fmt.Printf("Postgres replication %s removed successfully.\n", config.PostgresReplicationID)
	return nil
}
