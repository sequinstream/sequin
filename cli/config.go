package cli

// Config holds the global configuration for the CLI
type Config struct {
	Context string
}

// GetServerURL returns the server URL based on the current context
func (c *Config) GetServerURL() (string, error) {
	if c.Context == "" {
		return "http://localhost:4000", nil
	}

	ctx, err := LoadContext(c.Context)
	if err != nil {
		return "", err
	}

	return ctx.ServerURL, nil
}
