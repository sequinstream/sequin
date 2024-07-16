package cli

import (
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
	"github.com/choria-io/fisk"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
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
	sources := app.Command("source", "Source related commands")
	postgres := sources.Command("postgres", "Postgres replication related commands").Alias("pg").Alias("p")

	addCheat("postgres_replication", postgres)

	c := &postgresReplicationConfig{}

	add := postgres.Command("add", "Add a new postgres replication").Action(func(ctx *fisk.ParseContext) error {
		return postgresReplicationAdd(ctx, config, c)
	})
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

	streamID, err := getFirstStream(ctx)
	if err != nil {
		return err
	}
	c.StreamID = streamID

	databaseID, err := promptForDatabase(ctx)
	if err != nil {
		return err
	}

	hasExistingSetup, err := promptForExistingSetup()
	if err != nil {
		return err
	}

	if hasExistingSetup {
		err = promptForExistingReplicationDetails(c)
	} else {
		err = handleNewReplicationSetup(ctx, databaseID, c)
	}
	if err != nil {
		return err
	}

	// Create the postgres replication
	replication := api.PostgresReplicationCreate{
		SlotName:           c.SlotName,
		PublicationName:    c.PublicationName,
		StreamID:           c.StreamID,
		PostgresDatabaseID: databaseID,
	}

	newReplication, err := api.AddPostgresReplication(ctx, &replication)
	fisk.FatalIfError(err, "could not create Postgres replication")

	fmt.Printf("Postgres replication created successfully. ID: %s\n", newReplication.ID)

	// Show info for the new PGR
	config.PostgresReplicationID = newReplication.ID
	err = postgresReplicationInfo(nil, config)
	if err != nil {
		fmt.Printf("Warning: Could not fetch detailed information for the new replication: %v\n", err)
	}

	return nil
}

func promptForExistingSetup() (bool, error) {
	var hasExisting bool
	prompt := &survey.Confirm{
		Message: "Do you have an existing Postgres replication slot and publication for Sequin to connect to?\n(If you're not sure, you probably don't)",
	}
	err := survey.AskOne(prompt, &hasExisting)
	return hasExisting, err
}

func promptForExistingReplicationDetails(c *postgresReplicationConfig) error {
	questions := []*survey.Question{
		{
			Name: "SlotName",
			Prompt: &survey.Input{
				Message: "Enter the replication slot's name:",
				Help:    "The name of the replication slot you configured for Sequin to replicate from.",
			},
			Validate: survey.Required,
		},
		{
			Name: "PublicationName",
			Prompt: &survey.Input{
				Message: "Enter the publication name:",
				Help:    "The name of the publication you configured for Sequin to replicate from.",
			},
			Validate: survey.Required,
		},
	}
	return survey.Ask(questions, c)
}

func handleNewReplicationSetup(ctx *context.Context, databaseID string, c *postgresReplicationConfig) error {
	c.SlotName = "sequin_slot"
	c.PublicationName = "sequin_pub"

	fmt.Printf("We suggest the following defaults:\n")
	fmt.Printf("\033[1mSlot name\033[0m: %s\n", c.SlotName)
	fmt.Printf("\033[1mPublication name\033[0m: %s\n", c.PublicationName)

	var useDefaults bool
	prompt := &survey.Confirm{
		Message: "Do these defaults look OK?",
		Default: true,
	}
	err := survey.AskOne(prompt, &useDefaults)
	if err != nil {
		return err
	}

	if !useDefaults {
		err = promptForExistingReplicationDetails(c)
		if err != nil {
			return err
		}
	}

	var setupAutomatically bool
	prompt = &survey.Confirm{
		Message: "Do you want me to setup the replication slot and publication for you?",
		Default: true,
	}
	err = survey.AskOne(prompt, &setupAutomatically)
	if err != nil {
		return err
	}

	if setupAutomatically {
		schemas, err := api.ListSchemas(ctx, databaseID)
		if err != nil {
			return fmt.Errorf("failed to list schemas: %w", err)
		}

		var selectedSchema string
		err = survey.AskOne(&survey.Select{
			Message: "Choose a schema:",
			Options: schemas,
		}, &selectedSchema)
		if err != nil {
			return err
		}

		tables, err := api.ListTables(ctx, databaseID, selectedSchema)
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		var selectedTables []string
		for {
			err = survey.AskOne(&survey.MultiSelect{
				Message: "Select tables to sync (at least one):",
				Options: tables,
			}, &selectedTables)
			if err != nil {
				return err
			}

			if len(selectedTables) == 0 {
				fmt.Println("You must select at least one table. Please try again.")
			} else {
				break
			}
		}

		formattedTables := make([][]string, len(selectedTables))
		for i, table := range selectedTables {
			formattedTables[i] = []string{selectedSchema, table}
		}

		err = api.SetupReplicationSlotAndPublication(ctx, databaseID, c.SlotName, c.PublicationName, formattedTables)
		if err != nil {
			return fmt.Errorf("failed to setup replication slot and publication: %w", err)
		}
		fmt.Println("Replication slot and publication created successfully.")
	} else {
		showManualSetupInstructions(c.SlotName, c.PublicationName)
		fmt.Println("Press Enter when you have completed the setup.")
		fmt.Scanln() // Wait for user to press Enter
	}

	return nil
}

func showManualSetupInstructions(slotName, pubName string) {
	fmt.Printf(`
Please follow these steps to set up the replication slot and publication manually:

1. Connect to your PostgreSQL database as a superuser.

2. Create the replication slot:
   SELECT pg_create_logical_replication_slot('%s', 'pgoutput');

3. Create the publication:
   CREATE PUBLICATION %s FOR ALL TABLES;

After completing these steps, your database will be ready for Sequin to connect and start replicating.
`, slotName, pubName)
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
		fmt.Println("No sources defined")
		return nil
	}

	columns := []table.Column{
		{Title: "ID", Width: 36},
		{Title: "Slot Name", Width: 20},
		{Title: "Publication Name", Width: 20},
		{Title: "Status", Width: 10},
		{Title: "Stream ID", Width: 36},
	}

	rows := []table.Row{}
	for _, r := range replications {
		rows = append(rows, table.Row{r.ID, r.SlotName, r.PublicationName, r.Status, r.StreamID})
	}

	tableType := PrintableTable
	if len(rows) > 10 {
		tableType = InteractiveTable
	}

	t := NewTable(columns, rows, tableType)
	return t.Render()
}

func postgresReplicationInfo(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}
	id := config.PostgresReplicationID
	if id == "" {
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
		}, &id)
		if err != nil {
			return err
		}
		id = strings.Split(id, " ")[0]
	}

	replicationWithInfo, err := api.FetchPostgresReplicationInfo(ctx, id)
	if err != nil {
		return err
	}

	columns := []table.Column{
		{Title: "Field", Width: 30},
		{Title: "Value", Width: 50},
	}

	rows := []table.Row{
		{"ID", replicationWithInfo.PostgresReplication.ID},
		{"Slot Name", replicationWithInfo.PostgresReplication.SlotName},
		{"Publication Name", replicationWithInfo.PostgresReplication.PublicationName},
		{"Status", replicationWithInfo.PostgresReplication.Status},
		{"Stream ID", replicationWithInfo.PostgresReplication.StreamID},
		{"Postgres Database ID", replicationWithInfo.PostgresReplication.PostgresDatabaseID},
		{"Backfill Completed At", replicationWithInfo.PostgresReplication.FormatBackfillCompletedAt()},
		{"Last Committed Timestamp", replicationWithInfo.Info.FormatLastCommittedAt()},
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Printf("Information for Postgres Replication %s\n\n", replicationWithInfo.PostgresReplication.ID)
	return t.Render()
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
