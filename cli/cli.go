package cli

import (
	"embed"
)

var (
	//go:embed cheats
	fs embed.FS
)

type Config struct {
	ContextName           string
	StreamID              string
	PostgresReplicationID string
	ConsumerID            string
	AsCurl                bool
}
