package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/choria-io/fisk"
	"github.com/sequinstream/sequin/cli/api"
	sqcontext "github.com/sequinstream/sequin/cli/context"
)

type planCommand struct {
	yamlPath string
}

func AddPlanCommands(app *fisk.Application, config *Config) {
	cmd := &planCommand{}
	plan := app.Command("plan", "Plan changes from a YAML configuration file").
		Action(func(_ *fisk.ParseContext) error {
			return cmd.planAction(config)
		})

	plan.Arg("yaml-path", "Path to YAML file (defaults to sequin.yaml or sequin.yml)").
		StringVar(&cmd.yamlPath)

	addCheat("plan", plan)
}

func (c *planCommand) planAction(config *Config) error {
	// Load context
	sqctx, err := sqcontext.LoadContext(config.ContextName)
	if err != nil {
		return fmt.Errorf("could not load context: %w", err)
	}

	// Find YAML file
	yamlPath := c.yamlPath
	if yamlPath == "" {
		candidates := []string{"sequin.yaml", "sequin.yml"}
		for _, candidate := range candidates {
			if _, err := os.Stat(candidate); err == nil {
				yamlPath = candidate
				break
			}
		}
		if yamlPath == "" {
			return fmt.Errorf("no YAML file found (tried: %v)", candidates)
		}
	}

	// Read YAML file
	yamlContent, err := os.ReadFile(yamlPath)
	if err != nil {
		return fmt.Errorf("could not read YAML file: %w", err)
	}

	// Call API
	planResponse, err := api.PlanYaml(sqctx, string(yamlContent))
	if err != nil {
		return fmt.Errorf("plan failed: %w", err)
	}

	// Print changes
	fmt.Printf("Planned changes for %s:\n\n", filepath.Base(yamlPath))
	
	for _, changeset := range planResponse.Data {
		// Print entity type and name
		var name string
		if n, ok := changeset.Data["name"].(string); ok {
			name = n
		} else if n, ok := changeset.Changes["name"].(string); ok {
			name = n
		}

		fmt.Printf("• %s %s\n", changeset.Action, name)

		// Print changes
		if len(changeset.Changes) > 0 {
			fmt.Println("  Changes:")
			for key, value := range changeset.Changes {
				if key != "name" { // Skip name as we already showed it
					fmt.Printf("    - %s: %v\n", key, value)
				}
			}
		}

		// Print any errors
		if len(changeset.Errors) > 0 {
			fmt.Println("  Errors:")
			for key, value := range changeset.Errors {
				fmt.Printf("    - %s: %v\n", key, value)
			}
		}
		fmt.Println()
	}

	return nil
}