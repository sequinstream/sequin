package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/choria-io/fisk"

	chclient "github.com/sequinstream/sequin/cli/chisel/client"
	sqcontext "github.com/sequinstream/sequin/cli/context"
)

type tunnelCommand struct {
	ports []string
}

func AddTunnelCommands(app *fisk.Application, config *Config) {
	cmd := &tunnelCommand{}
	tunnel := app.Command("tunnel", "Create tunnels to Sequin").Action(func(_ *fisk.ParseContext) error {
		return cmd.tunnelAction(config)
	})

	// Add cheats
	addCheat("tunnel", tunnel)

	// Add flags
	tunnel.Flag("ports", "Ports to tunnel, in the format <local-port>:<name-or-id>[,<local-port>:<name-or-id>]").
		Required().
		StringsVar(&cmd.ports)
}

func (c *tunnelCommand) tunnelAction(config *Config) error {
	// Load context
	sqctx, err := sqcontext.LoadContext(config.ContextName)
	if err != nil {
		return fmt.Errorf("could not load context: %w", err)
	}

	// Prepare remotes
	var remotes []string
	for _, portMapping := range c.ports {
		portPairs := strings.Split(portMapping, ",")
		for _, pair := range portPairs {
			parts := strings.Split(pair, ":")
			if len(parts) != 2 {
				return fmt.Errorf("invalid port mapping: %s", pair)
			}
			localPort := parts[0]
			nameOrID := parts[1]
			remote := fmt.Sprintf("R:%s:localhost:%s", localPort, nameOrID)
			remotes = append(remotes, remote)
		}
	}

	// Build client configuration
	clientConfig := &chclient.Config{
		Server:      sqctx.PortalBaseURL,
		Remotes:     remotes,
		Auth:        fmt.Sprintf("notused:%s", sqctx.ApiToken),
		Fingerprint: "", // Set this if you need to verify the server's fingerprint
		KeepAlive:   25 * time.Second,
	}

	// Create a new client
	chiselClient, err := chclient.NewClient(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create chisel client: %w", err)
	}

	// Create a context that listens for the interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		fmt.Println("\nInterrupt received, shutting down...")
		cancel()
		chiselClient.Close()
	}()

	// Start the client
	if err := chiselClient.Start(ctx); err != nil {
		return fmt.Errorf("failed to start chisel client: %w", err)
	}

	// Wait for the client to finish
	if err := chiselClient.Wait(); err != nil {
		return fmt.Errorf("chisel client error: %w", err)
	}

	return nil
}
