package cli

import (
	"fmt"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
)

func addCheat(name string, cmd *fisk.CmdClause) {
	cmd.CheatFile(fs, name, fmt.Sprintf("cheats/%s.md", name))
}

func askConfirmation(prompt string, dflt bool) (bool, error) {
	ans := dflt

	err := survey.AskOne(&survey.Confirm{
		Message: prompt,
		Default: dflt,
	}, &ans)

	return ans, err
}
