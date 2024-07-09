package cli

import (
	"fmt"

	"github.com/choria-io/fisk"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

func addCheat(name string, cmd *fisk.CmdClause) {
	cmd.CheatFile(fs, name, fmt.Sprintf("cheats/%s.md", name))
}

func newTableWriter(format string, a ...any) *tbl {
	tbl := &tbl{
		writer: table.NewWriter(),
	}

	tbl.writer.SetStyle(styles["rounded"])

	tbl.writer.Style().Title.Align = text.AlignCenter
	tbl.writer.Style().Format.Header = text.FormatDefault
	tbl.writer.Style().Format.Footer = text.FormatDefault

	if format != "" {
		tbl.writer.SetTitle(fmt.Sprintf(format, a...))
	}

	return tbl
}
