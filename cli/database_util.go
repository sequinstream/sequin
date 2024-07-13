package cli

import (
	"fmt"
	"strconv"
	"strings"

	"sequin-cli/api"
	"sequin-cli/context"

	"github.com/AlecAivazis/survey/v2"
)

func promptForNewDatabase(ctx *context.Context) (*api.PostgresDatabase, error) {
	var database api.PostgresDatabaseCreate
	var portStr string

	err := survey.Ask([]*survey.Question{
		{
			Name: "Database",
			Prompt: &survey.Input{
				Message: "Enter the dbname for the source database:",
				Default: "postgres",
			},
			Validate: survey.Required,
		},
		{
			Name: "Hostname",
			Prompt: &survey.Input{
				Message: "Enter the hostname for the source database:",
				Default: "localhost",
			},
			Validate: survey.Required,
		},
		{
			Name: "Port",
			Prompt: &survey.Input{
				Message: "Enter the port for the source database:",
				Default: "5432",
			},
			Validate: survey.Required,
		},
		{
			Name: "Username",
			Prompt: &survey.Input{
				Message: "Enter the username for the source database:",
				Default: "postgres",
			},
			Validate: survey.Required,
		},
		{
			Name: "Password",
			Prompt: &survey.Password{
				Message: "Enter the password for the source database:",
			},
			Validate: survey.Required,
		},
		{
			Name: "Slug",
			Prompt: &survey.Input{
				Message: "Enter a slug for the database:",
				Help:    "A unique identifier for this database.",
			},
			Validate: survey.Required,
		},
	}, &database)

	if err != nil {
		return nil, err
	}
	// Convert string port to int or use default
	port := 5432
	if portStr != "" {
		var err error
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port number: %v", err)
		}
	}
	database.Port = port

	newDatabase, err := api.AddPostgresDatabase(ctx, &database)
	if err != nil {
		return nil, err
	}

	return newDatabase, nil
}

func promptForDatabase(ctx *context.Context) (string, error) {
	databases, err := api.FetchPostgresDatabases(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch databases: %w", err)
	}

	databaseOptions := make([]string, len(databases)+1)
	databaseOptions[0] = "Create new database"
	for i, db := range databases {
		databaseOptions[i+1] = fmt.Sprintf("%s (ID: %s)", db.Slug, db.ID)
	}

	var choice string
	err = survey.AskOne(&survey.Select{
		Message: "Choose a database or create a new one:",
		Options: databaseOptions,
		Filter: func(filterValue string, optValue string, index int) bool {
			return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
		},
	}, &choice)
	if err != nil {
		return "", fmt.Errorf("failed to get user input: %w", err)
	}

	if choice == "Create new database" {
		newDatabase, err := promptForNewDatabase(ctx)
		if err != nil {
			return "", err
		}
		return newDatabase.ID, nil
	}

	// Extract the database ID from the selected option
	parts := strings.Split(choice, "(ID: ")
	if len(parts) == 2 {
		return strings.TrimRight(parts[1], ")"), nil
	}
	return "", fmt.Errorf("invalid database choice format")
}
