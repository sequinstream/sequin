package cli

import (
	"embed"
)

var (
	//go:embed cheats
	fs embed.FS
)

type Config struct {
	ContextName string
}

const LogFilePath = "sequin.log"
