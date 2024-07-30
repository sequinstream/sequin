package main

import (
	"log"
	"os"
	"runtime/debug"

	"github.com/choria-io/fisk"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/cli"
)

var (
	version = "development"
	config  = cli.Config{}
)

func main() {
	// Set up logging
	logFile, err := os.OpenFile("sequin.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Println("Starting Sequin CLI")

	help := `Sequin CLI

See 'sequin cheat' for a quick cheatsheet of commands`

	scli := fisk.New("sequin", help)
	scli.Author("Sequin Authors <founders@sequin.io>")
	scli.UsageWriter(os.Stdout)
	scli.Version(getVersion())
	scli.HelpFlag.Short('h')
	scli.WithCheats().CheatCommand.Hidden()

	// Initialize the API client
	apiClient := api.NewClient()

	// Add global context flag
	scli.Flag("context", "Use a specific context").StringVar(&config.ContextName)
	scli.Flag("as-curl", "Output the command as a curl request instead of executing it").BoolVar(&config.AsCurl)

	cli.AddContextCommands(scli, &config)
	cli.AddStreamCommands(scli, &config, apiClient)
	cli.AddSourceCommands(scli, &config)
	cli.AddConsumerCommands(scli, &config)
	cli.AddObserveCommands(scli, &config)

	log.Println("Parsing command line arguments")
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
