package cli

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/choria-io/fisk"
	"github.com/fatih/color"
	"github.com/r3labs/diff/v3"
	"github.com/sequinstream/sequin/cli/config"
	"github.com/sequinstream/sequin/cli/context"
)

type ConfigCommands struct {
	config   *Config
	yamlPath string
}

// AddYamlCommands adds the 'plan' and 'apply' commands for YAML-based operations
func AddConfigCommands(app *fisk.Application, cfg *Config) {
	cmd := &ConfigCommands{config: cfg}

	// Add yaml command group
	config := app.Command("config", "Config-based operations")

	// Plan command
	plan := config.Command("plan", "Show changes that would be applied from YAML file")
	plan.Arg("file", "Path to YAML file").
		Default("sequin.yaml").
		StringVar(&cmd.yamlPath)
	plan.Action(cmd.planAction)

	// Apply command
	apply := config.Command("apply", "Apply changes from YAML file")
	apply.Arg("file", "Path to YAML file").
		Default("sequin.yaml").
		StringVar(&cmd.yamlPath)
	apply.Action(cmd.applyAction)
}

func (c *ConfigCommands) planAction(_ *fisk.ParseContext) error {
	// Load current context
	ctx, err := context.LoadContext("")
	if err != nil {
		return fmt.Errorf("failed to load context: %w", err)
	}

	// Call plan
	planResp, err := config.Plan(ctx, c.yamlPath)
	if err != nil {
		return err
	}

	// Display results
	if len(planResp.Changes) == 0 {
		fmt.Println("No changes detected\r")
		return nil
	}

	// Track counts
	creates, updates, deletes := 0, 0, 0

	// Setup colors and symbols
	plus := color.New(color.FgGreen).Sprint("+")
	minus := color.New(color.FgRed).Sprint("-")
	tilde := color.New(color.FgYellow).Sprint("~")
	bold := color.New(color.Bold).SprintFunc()

	fmt.Printf("\nSequin will perform the following actions:\n\n")

	for _, change := range planResp.Changes {
		// Update counts
		switch change.Action {
		case "create":
			creates++
		case "update":
			updates++
		case "delete":
			deletes++
		}

		// Print resource header with appropriate symbol
		symbol := tilde
		if change.Action == "create" {
			symbol = plus
		} else if change.Action == "delete" {
			symbol = minus
		}

		fmt.Printf("%s %s %s\n", symbol, bold(change.ResourceType), change.Action)

		// Convert and show diff
		var oldMap, newMap map[string]interface{}
		if change.Old != nil {
			oldMap = convertToMap(change.Old)
		}
		if change.New != nil {
			newMap = convertToMap(change.New)
		}

		if oldMap != nil || newMap != nil {
			diff := PrettyDiff(oldMap, newMap)
			fmt.Print(diff)
		}
		fmt.Println()
	}

	// Print summary
	fmt.Printf("\nPlan: %d to add, %d to change, %d to destroy.\n",
		creates, updates, deletes)

	return nil
}

// Helper function to convert struct to map
func convertToMap(v interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Use reflection to convert struct to map
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Map {
		// If it's already a map, just type assert it
		if m, ok := v.(map[string]interface{}); ok {
			return m
		}
	}

	return result
}

func (c *ConfigCommands) applyAction(_ *fisk.ParseContext) error {
	// TODO: Implement apply logic
	return nil
}

// PrettyDiff generates a colored diff output between two maps
func PrettyDiff(oldMap, newMap map[string]interface{}) string {
	// Create diff engine
	differ, err := diff.NewDiffer(diff.AllowTypeMismatch(true))
	if err != nil {
		return fmt.Sprintf("Error creating differ: %v", err)
	}

	// Calculate changes
	changes, err := differ.Diff(oldMap, newMap)
	if err != nil {
		return fmt.Sprintf("Error calculating diff: %v", err)
	}

	if len(changes) == 0 {
		return "No changes detected"
	}

	// Setup colors
	added := color.New(color.FgGreen).SprintFunc()
	removed := color.New(color.FgRed).SprintFunc()
	modified := color.New(color.FgYellow).SprintFunc()
	unchanged := color.New(color.FgHiBlack).SprintFunc()

	var builder strings.Builder

	// For creates (only newMap exists)
	if oldMap == nil && newMap != nil {
		for k, v := range newMap {
			builder.WriteString(fmt.Sprintf("  %s: %v\n", k, formatValue(v)))
		}
		return builder.String()
	}

	// For deletes (only oldMap exists)
	if oldMap != nil && newMap == nil {
		for k, v := range oldMap {
			builder.WriteString(fmt.Sprintf("  %s: %v\n", k, formatValue(v)))
		}
		return builder.String()
	}

	// For updates
	if oldMap != nil && newMap != nil {
		// Get all keys
		allKeys := make(map[string]bool)
		for k := range oldMap {
			allKeys[k] = true
		}
		for k := range newMap {
			allKeys[k] = true
		}

		// Sort keys for consistent output
		var keys []string
		for k := range allKeys {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			oldVal, oldOk := oldMap[k]
			newVal, newOk := newMap[k]

			if !oldOk {
				// New field
				builder.WriteString(fmt.Sprintf("  %s: %v\n",
					added(k), added(formatValue(newVal))))
			} else if !newOk {
				// Deleted field
				builder.WriteString(fmt.Sprintf("  %s: %v\n",
					removed(k), removed(formatValue(oldVal))))
			} else if oldVal != newVal {
				// Changed field
				builder.WriteString(fmt.Sprintf("  %s: %v → %v\n",
					modified(k), modified(formatValue(oldVal)), modified(formatValue(newVal))))
			} else {
				// Unchanged field
				builder.WriteString(fmt.Sprintf("  %s: %v\n",
					unchanged(k), unchanged(formatValue(oldVal))))
			}
		}
	}

	return builder.String()
}

// filterChangesByPath returns all changes that start with the given path
func filterChangesByPath(changes []diff.Change, path string) []diff.Change {
	var filtered []diff.Change
	for _, change := range changes {
		if len(change.Path) > 0 && change.Path[0] == path {
			filtered = append(filtered, change)
		}
	}
	return filtered
}

// formatValue converts a value to a readable string
func formatValue(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v", v)
}
