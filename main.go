package main

import (
	"log"
	"os"
	"runtime/debug"

	"github.com/choria-io/fisk"

	"sequin-cli/cli"
)

var (
	version = "development"
	config  = cli.Config{}
)

func main() {
	help := `Sequin Utility

Sequin Server and JetStream administration.

See 'nats cheat' for a quick cheatsheet of commands`

	scli := fisk.New("sequin", help)
	scli.Author("Sequin Authors <founders@sequin.io>")
	scli.UsageWriter(os.Stdout)
	scli.Version(getVersion())
	scli.HelpFlag.Short('h')
	scli.WithCheats().CheatCommand.Hidden()

	// Add global context flag
	scli.Flag("context", "Use a specific context").StringVar(&config.Context)

	cli.AddStreamCommands(scli, &config)
	cli.AddContextCommands(scli, &config)

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
