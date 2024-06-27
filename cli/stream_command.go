package cli

import (
	"fmt"

	"github.com/choria-io/fisk"
)

// AddStreamCommands adds all stream-related commands to the given app
func AddStreamCommands(app *fisk.Application) {
	streamCmd := app.Command("stream", "Stream related commands")

	streamCmd.Command("ls", "List streams").Action(func(c *fisk.ParseContext) error {
		return streamLs(c)
	})
}

func streamLs(_ *fisk.ParseContext) error {
	fmt.Println("not implemented")
	return nil
}
