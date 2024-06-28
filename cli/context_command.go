package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/choria-io/fisk"
)

type Context struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	ServerURL   string `json:"server_url"`
	// Add other fields as needed
}

type ctxCommand struct {
	name        string
	description string
	serverURL   string
}

func AddContextCommands(app *fisk.Application, _ *Config) {
	cmd := &ctxCommand{}
	ctx := app.Command("context", "Manage sequin configuration contexts")

	create := ctx.Command("create", "Create or update a context").Action(cmd.createAction)
	create.Arg("name", "The context name").Required().StringVar(&cmd.name)
	create.Flag("description", "Set a friendly description for this context").StringVar(&cmd.description)
	create.Flag("server-url", "The server URL for this context").Required().StringVar(&cmd.serverURL)

	// Add more subcommands like list, show, delete as needed
}

func (c *ctxCommand) createAction(_ *fisk.ParseContext) error {
	ctx := Context{
		Name:        c.name,
		Description: c.description,
		ServerURL:   c.serverURL,
	}

	err := saveContext(ctx)
	if err != nil {
		return fmt.Errorf("could not save context: %w", err)
	}

	fmt.Printf("Context '%s' created successfully.\n", c.name)
	return nil
}

func saveContext(ctx Context) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("could not get user home directory: %w", err)
	}

	dir := filepath.Join(home, ".sequin", "contexts")
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("could not create contexts directory: %w", err)
	}

	file := filepath.Join(dir, ctx.Name+".json")
	data, err := json.MarshalIndent(ctx, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal context: %w", err)
	}

	err = os.WriteFile(file, data, 0644)
	if err != nil {
		return fmt.Errorf("could not write context file: %w", err)
	}

	return nil
}

func LoadContext(name string) (*Context, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("could not get user home directory: %w", err)
	}

	file := filepath.Join(home, ".sequin", "contexts", name+".json")
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("could not read context file: %w", err)
	}

	var ctx Context
	err = json.Unmarshal(data, &ctx)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal context: %w", err)
	}

	return &ctx, nil
}
