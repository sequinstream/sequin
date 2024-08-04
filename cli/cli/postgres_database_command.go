package cli

import (
	"encoding/json"
	"fmt"

	"github.com/charmbracelet/bubbles/table"
	"github.com/choria-io/fisk"
	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
)

type postgresDatabaseConfig struct {
	ID            string
	Database      string
	Hostname      string
	PoolSize      int
	Port          int
	QueueInterval int
	QueueTarget   int
	Name          string
	SSL           bool
	Username      string
	Password      string
	AsJSON        bool
}

func AddPostgresDatabaseCommands(app *fisk.Application, config *Config) {
	postgres := app.Command("postgres", "Postgres database related commands").Alias("pg")

	addCheat("postgres", postgres)
	p := &postgresDatabaseConfig{}

	postgres.Command("ls", "List postgres databases").Action(func(ctx *fisk.ParseContext) error {
		return postgresDatabaseList(ctx, config)
	})

	addCmd := postgres.Command("add", "Add a new postgres database").Action(func(ctx *fisk.ParseContext) error {
		return postgresDatabaseAdd(ctx, config, p)
	})
	addCmd.Flag("database", "Database name").StringVar(&p.Database)
	addCmd.Flag("hostname", "Database hostname").StringVar(&p.Hostname)
	addCmd.Flag("port", "Database port").IntVar(&p.Port)
	addCmd.Flag("username", "Database username").StringVar(&p.Username)
	addCmd.Flag("password", "Database password").StringVar(&p.Password)
	addCmd.Flag("name", "Friendly name for the database").StringVar(&p.Name)
	addCmd.Flag("ssl", "Use SSL for connection").BoolVar(&p.SSL)
	addCmd.Flag("json", "JSON string containing postgres database configuration").StringVar(&config.JSONInput)
	addCmd.Flag("json-file", "Path to JSON file containing postgres database configuration").StringVar(&config.JSONFile)

	infoCmd := postgres.Command("info", "Show postgres database information").Action(func(ctx *fisk.ParseContext) error {
		return postgresDatabaseInfo(ctx, config, p)
	})
	infoCmd.Arg("id", "ID of the postgres database").StringVar(&p.ID)
	infoCmd.Flag("as-json", "Print postgres database info as JSON").BoolVar(&p.AsJSON)

	editCmd := postgres.Command("edit", "Edit an existing postgres database").Action(func(ctx *fisk.ParseContext) error {
		return postgresDatabaseEdit(ctx, config, p)
	})
	editCmd.Arg("id", "ID of the postgres database to edit").StringVar(&p.ID)
	editCmd.Flag("json", "JSON string containing postgres database configuration").StringVar(&config.JSONInput)
	editCmd.Flag("json-file", "Path to JSON file containing postgres database configuration").StringVar(&config.JSONFile)

	removeCmd := postgres.Command("remove", "Remove a postgres database").Action(func(ctx *fisk.ParseContext) error {
		return postgresDatabaseRemove(ctx, config, p)
	})
	removeCmd.Arg("id", "ID of the postgres database to remove").StringVar(&p.ID)
	removeCmd.Flag("force", "Force removal without confirmation").BoolVar(&config.Force)
}

func postgresDatabaseList(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildFetchPostgresDatabases(ctx)
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

	databases, err := api.FetchPostgresDatabases(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch postgres databases: %w", err)
	}

	columns := []table.Column{
		{Title: "ID", Width: 36},
		{Title: "Name", Width: 30},
		{Title: "Database", Width: 20},
		{Title: "Hostname", Width: 30},
		{Title: "Port", Width: 10},
		{Title: "SSL", Width: 5},
	}

	rows := make([]table.Row, len(databases))
	for i, db := range databases {
		rows[i] = table.Row{
			db.ID,
			db.Name,
			db.Database,
			db.Hostname,
			fmt.Sprintf("%d", db.Port),
			fmt.Sprintf("%t", db.SSL),
		}
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Println("Postgres Databases")
	return t.Render()
}

func postgresDatabaseAdd(_ *fisk.ParseContext, config *Config, p *postgresDatabaseConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	var newDatabase *api.PostgresDatabase

	if config.JSONFile != "" || config.JSONInput != "" {
		databaseCreate := &api.PostgresDatabaseCreate{}
		if err := MergeJSONConfig(databaseCreate, config.JSONInput, config.JSONFile); err != nil {
			return err
		}
		newDatabase, err = api.AddPostgresDatabase(ctx, databaseCreate)
		if err != nil {
			return fmt.Errorf("failed to create new database: %w", err)
		}
	} else {
		newDatabase, err = promptForNewDatabase(ctx)
		if err != nil {
			return err
		}
	}

	fmt.Println("Postgres database created successfully:")
	return displayPostgresDatabaseInfo(newDatabase, p.AsJSON)
}

func postgresDatabaseInfo(_ *fisk.ParseContext, config *Config, p *postgresDatabaseConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if p.ID == "" {
		p.ID, err = promptForPostgresDatabase(ctx)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildFetchPostgresDatabaseInfo(ctx, p.ID)
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

	database, err := api.FetchPostgresDatabaseInfo(ctx, p.ID)
	if err != nil {
		return fmt.Errorf("failed to fetch postgres database info: %w", err)
	}

	return displayPostgresDatabaseInfo(database, p.AsJSON)
}

func postgresDatabaseEdit(_ *fisk.ParseContext, config *Config, p *postgresDatabaseConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if p.ID == "" {
		p.ID, err = promptForDatabase(ctx)
		if err != nil {
			return err
		}
	}

	var updatedDatabase *api.PostgresDatabase

	if config.JSONFile != "" || config.JSONInput != "" {
		databaseUpdate := &api.PostgresDatabaseUpdate{}
		if err := MergeJSONConfig(databaseUpdate, config.JSONInput, config.JSONFile); err != nil {
			return err
		}
		updatedDatabase, err = api.UpdatePostgresDatabase(ctx, p.ID, databaseUpdate)
		if err != nil {
			return fmt.Errorf("failed to update database: %w", err)
		}
	} else {
		currentDatabase, err := api.FetchPostgresDatabaseInfo(ctx, p.ID)
		if err != nil {
			return fmt.Errorf("failed to fetch current database info: %w", err)
		}

		updatedDatabaseCreate, err := promptForEditDatabase(ctx, currentDatabase)
		if err != nil {
			return err
		}

		databaseUpdate := &api.PostgresDatabaseUpdate{
			Database:      updatedDatabaseCreate.Database,
			Hostname:      updatedDatabaseCreate.Hostname,
			PoolSize:      updatedDatabaseCreate.PoolSize,
			Port:          updatedDatabaseCreate.Port,
			QueueInterval: updatedDatabaseCreate.QueueInterval,
			QueueTarget:   updatedDatabaseCreate.QueueTarget,
			Name:          updatedDatabaseCreate.Name,
			SSL:           updatedDatabaseCreate.SSL,
			Username:      updatedDatabaseCreate.Username,
			Password:      updatedDatabaseCreate.Password,
		}

		updatedDatabase, err = api.UpdatePostgresDatabase(ctx, p.ID, databaseUpdate)
		if err != nil {
			return fmt.Errorf("failed to update database: %w", err)
		}
	}

	fmt.Println("Postgres database updated successfully:")
	return displayPostgresDatabaseInfo(updatedDatabase, p.AsJSON)
}

func postgresDatabaseRemove(_ *fisk.ParseContext, config *Config, p *postgresDatabaseConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if p.ID == "" {
		p.ID, err = promptForPostgresDatabase(ctx)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildRemovePostgresDatabase(ctx, p.ID)
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

	if !config.Force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove Postgres Database %s", p.ID), false)
		if err != nil {
			return fmt.Errorf("could not obtain confirmation: %w", err)
		}
		if !ok {
			return nil
		}
	}

	err = api.RemovePostgresDatabase(ctx, p.ID)
	if err != nil {
		return fmt.Errorf("failed to remove postgres database: %w", err)
	}

	fmt.Printf("Postgres Database %s removed successfully\n", p.ID)
	return nil
}

func promptForPostgresDatabase(ctx *context.Context) (string, error) {
	return promptForDatabase(ctx)
}

func displayPostgresDatabaseInfo(database interface{}, asJSON bool) error {
	if asJSON {
		jsonData, err := json.MarshalIndent(database, "", "  ")
		if err != nil {
			return fmt.Errorf("error marshaling postgres database to JSON: %w", err)
		}
		fmt.Println(string(jsonData))
		return nil
	}

	columns := []table.Column{
		{Title: "Field", Width: 20},
		{Title: "Value", Width: 40},
	}

	var rows []table.Row

	switch d := database.(type) {
	case *api.PostgresDatabase:
		rows = []table.Row{
			{"ID", d.ID},
			{"Name", d.Name},
			{"Database", d.Database},
			{"Hostname", d.Hostname},
			{"Port", fmt.Sprintf("%d", d.Port)},
			{"Username", d.Username},
			{"SSL", fmt.Sprintf("%t", d.SSL)},
			{"Pool Size", fmt.Sprintf("%d", d.PoolSize)},
			{"Queue Interval", fmt.Sprintf("%d", d.QueueInterval)},
			{"Queue Target", fmt.Sprintf("%d", d.QueueTarget)},
		}
	case *api.PostgresDatabaseCreate:
		rows = []table.Row{
			{"Name", d.Name},
			{"Database", d.Database},
			{"Hostname", d.Hostname},
			{"Port", fmt.Sprintf("%d", d.Port)},
			{"Username", d.Username},
			{"SSL", fmt.Sprintf("%t", d.SSL)},
		}
	default:
		return fmt.Errorf("unsupported database type")
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Printf("Postgres Database: %s\n", rows[1][1]) // Assuming "Name" is always the second row
	return t.Render()
}
