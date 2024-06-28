package cli

import (
	"embed"
	"sequin-cli/context"
)

var (
	//go:embed cheats
	fs embed.FS
)

type Config struct {
	Context  context.Context
	StreamID string
}
