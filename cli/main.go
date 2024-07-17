//go:build !prod
// +build !prod

package main

import (
	"log"
	"os"
	"runtime/debug"

	"github.com/choria-io/fisk"

	"github.com/sequinstream/sequin/cli/cli"
)

var (
	version = "development"
	config  = cli.Config{}
)

func main() {
	help := `Sequin CLI

See 'sequin cheat' for a quick cheatsheet of commands`

	scli := fisk.New("sequin", help)
	scli.Author("Sequin Authors <founders@sequin.io>")
	scli.UsageWriter(os.Stdout)
	scli.Version(getVersion())
	scli.HelpFlag.Short('h')
	scli.WithCheats().CheatCommand.Hidden()

	// Add global context flag
	scli.Flag("context", "Use a specific context").StringVar(&config.ContextName)
	scli.Flag("as-curl", "Output the command as a curl request instead of executing it").BoolVar(&config.AsCurl)

	cli.AddContextCommands(scli, &config)
	cli.AddStreamCommands(scli, &config)
	cli.AddPostgresReplicationCommands(scli, &config)
	cli.AddConsumerCommands(scli, &config)
	cli.AddObserveCommands(scli, &config)

	log.SetFlags(log.Ltime)

	scli.MustParseWithUsage(os.Args[1:])
}

func getVersion() string {
	if version != "development" {
		return version
	}

	nfo, ok := debug.ReadBuildInfo()
	if !ok || (nfo != nil && nfo.Main.Version == "") {
		return version
	}

	return nfo.Main.Version
}
