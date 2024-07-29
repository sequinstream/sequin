package context

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Context struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	ServerURL    string `json:"server_url"`
	WebSocketURL string `json:"websocket_url"`
}

// GetServerURL returns the server URL based on the current context
func GetServerURL(ctx *Context) (string, error) {
	if ctx.ServerURL == "" {
		return "", fmt.Errorf("server URL is not set")
	}

	return ctx.ServerURL, nil
}

func SaveContext(ctx Context) error {
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
	if name == "" {
		defaultName, err := getDefaultContextName()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// Return default context if no name is specified and no default set
				return &Context{
					Name:         "default",
					Description:  "default context",
					ServerURL:    fmt.Sprintf("http://localhost:%d", defaultPort),
					WebSocketURL: fmt.Sprintf("ws://localhost:%d/cli", defaultPort),
				}, nil
			}
			return nil, err
		}
		name = defaultName
	}

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

func ListContexts() ([]Context, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("could not get user home directory: %w", err)
	}

	dir := filepath.Join(home, ".sequin", "contexts")
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("could not read contexts directory: %w", err)
	}

	var contexts []Context
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			ctx, err := LoadContext(strings.TrimSuffix(file.Name(), ".json"))
			if err != nil {
				return nil, fmt.Errorf("could not load context %s: %w", file.Name(), err)
			}
			contexts = append(contexts, *ctx)
		}
	}

	return contexts, nil
}

func RemoveContext(name string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("could not get user home directory: %w", err)
	}

	file := filepath.Join(home, ".sequin", "contexts", name+".json")
	err = os.Remove(file)
	if err != nil {
		return fmt.Errorf("could not remove context file: %w", err)
	}

	defaultName, err := getDefaultContextName()
	if err == nil && defaultName == name {
		err = removeDefaultContext()
		if err != nil {
			return fmt.Errorf("could not remove default context: %w", err)
		}
	}

	return nil
}

const defaultContextFile = ".default_context"

func SetDefaultContext(name string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("could not get user home directory: %w", err)
	}

	dir := filepath.Join(home, ".sequin")
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return fmt.Errorf("could not create .sequin directory: %w", err)
	}

	file := filepath.Join(dir, defaultContextFile)
	err = os.WriteFile(file, []byte(name), 0644)
	if err != nil {
		return fmt.Errorf("could not write default context file: %w", err)
	}

	return nil
}

func getDefaultContextName() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("could not get user home directory: %w", err)
	}

	file := filepath.Join(home, ".sequin", defaultContextFile)
	data, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func removeDefaultContext() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("could not get user home directory: %w", err)
	}

	file := filepath.Join(home, ".sequin", defaultContextFile)
	err = os.Remove(file)
	if err != nil {
		return fmt.Errorf("could not remove default context file: %w", err)
	}

	return nil
}
