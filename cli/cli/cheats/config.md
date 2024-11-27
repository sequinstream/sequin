// Export current configuration to YAML
sequin config export > sequin.yaml

// Preview changes from a YAML file
sequin config plan
sequin config plan custom-config.yaml

// Apply changes from a YAML file
sequin config apply
sequin config apply custom-config.yaml

// Use with a specific context
sequin --context=prod config plan
sequin --context=prod config apply

// Common workflow
sequin config export > sequin.yaml  # Export current config
vim sequin.yaml                     # Make changes
sequin config plan                  # Review changes
sequin config apply                 # Apply changes