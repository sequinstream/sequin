package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime/debug"

	"github.com/choria-io/fisk"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/cli"
	"github.com/sequinstream/sequin/cli/constants"
)

var (
	version = "development"
	config  = cli.Config{}
)

func main() {
	// Set up logging
	logFile, err := os.OpenFile(constants.LogFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	defer logFile.Close()

	// Trim log file to latest 1000 lines
	if err := trimLogFile(logFile, 1000); err != nil {
		log.Println("Failed to trim log file:", err)
	}

	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Println("Starting Sequin CLI version:", getVersion())

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
	cli.AddReportCommand(scli, &config)

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

func trimLogFile(file *os.File, maxLines int) error {
	// Read all lines
	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	// Keep only the last maxLines
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}

	// Truncate the file
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}

	// Write the trimmed lines back to the file
	writer := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(writer, line)
	}
	return writer.Flush()
}
