package cli

import (
	"fmt"

	"github.com/choria-io/fisk"
)

func addCheat(name string, cmd *fisk.CmdClause) {
	cmd.CheatFile(fs, name, fmt.Sprintf("cheats/%s.md", name))
}
