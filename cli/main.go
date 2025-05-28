package main

import (
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
	scli.Author("Sequin Authors <support@sequinstream.com>")
	scli.UsageWriter(os.Stdout)
	scli.Version(getVersion())
	scli.HelpFlag.Short('h')
	scli.WithCheats().CheatCommand.Hidden()

	// Add global context flag
	scli.Flag("context", "Use a specific context").StringVar(&config.ContextName)

	cli.AddContextCommands(scli, &config)
	cli.AddTunnelCommands(scli, &config)
	cli.AddConfigCommands(scli, &config)

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
