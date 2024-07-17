package cli

import (
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type TableType int

type tickMsg time.Time

const (
	PrintableTable TableType = iota
	InteractiveTable
)

func NewTable(columns []table.Column, rows []table.Row, tableType TableType) *Table {
	options := []table.Option{
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithHeight(len(rows)),
	}

	if tableType == InteractiveTable {
		options = append(options, table.WithFocused(true))
	}

	t := table.New(options...)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)

	if tableType == InteractiveTable {
		s.Selected = s.Selected.
			Foreground(lipgloss.Color("229")).
			Background(lipgloss.Color("57")).
			Bold(false)
	}

	t.SetStyles(s)

	return &Table{
		table: t,
		typ:   tableType,
	}
}

type Table struct {
	table table.Model
	typ   TableType
}

func (t *Table) Init() tea.Cmd {
	return nil
}

func (t *Table) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			return t, tea.Quit
		}
	}
	t.table, cmd = t.table.Update(msg)
	return t, cmd
}

func (t *Table) View() string {
	return "\n" + t.table.View() + "\n"
}

func (t *Table) Render() error {
	switch t.typ {
	case PrintableTable:
		fmt.Println(t.View())
		return nil
	case InteractiveTable:
		_, err := tea.NewProgram(t).Run()
		return err
	default:
		return fmt.Errorf("unknown table type")
	}
}
