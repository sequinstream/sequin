package cli

import (
	"embed"
)

var (
	//go:embed cheats
	fs embed.FS
)

type Config struct {
	AsCurl                bool
	ConsumerID            string
	ContextName           string
	JSONFile              string
	JSONInput             string
	PostgresReplicationID string
	StreamID              string
	Force                 bool
}

const LogFilePath = "sequin.log"
