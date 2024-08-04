package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type formModel struct {
	inputs     []textinput.Model
	labels     []string
	focusIndex int
	err        error
	ctx        *context.Context
	database   *api.PostgresDatabaseCreate
	sslToggle  bool
	id         string
}

func initialDatabaseModel(ctx *context.Context, existingDatabase *api.PostgresDatabase) formModel {
	inputs := make([]textinput.Model, 6)
	labels := []string{
		"Database name:",
		"Hostname:",
		"Port:",
		"Username:",
		"Password:",
		"Give your database a name:",
	}
	for i := range inputs {
		t := textinput.New()
		switch i {
		case 0:
			t.Placeholder = "postgres"
			t.Focus()
		case 1:
			t.Placeholder = "localhost"
		case 2:
			t.Placeholder = "5432"
		case 3:
			t.Placeholder = "postgres"
		case 4:
			t.EchoMode = textinput.EchoPassword
			t.EchoCharacter = '•'
		}
		inputs[i] = t
	}

	var id string
	sslToggle := false

	if existingDatabase != nil {
		inputs[0].SetValue(existingDatabase.Database)
		inputs[1].SetValue(existingDatabase.Hostname)
		inputs[2].SetValue(fmt.Sprintf("%d", existingDatabase.Port))
		inputs[3].SetValue(existingDatabase.Username)
		inputs[5].SetValue(existingDatabase.Name)
		sslToggle = existingDatabase.SSL
		id = existingDatabase.ID
	}

	return formModel{
		inputs:     inputs,
		labels:     labels,
		focusIndex: 0,
		ctx:        ctx,
		sslToggle:  sslToggle,
		id:         id,
	}
}

func (m formModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m formModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			return m, tea.Quit
		case "ctrl+s":
			return m, m.submit
		case "tab", "shift+tab", "up", "down":
			s := msg.String()

			if s == "up" || s == "shift+tab" {
				m.focusIndex--
			} else {
				m.focusIndex++
			}

			if m.focusIndex > len(m.inputs)+1 {
				m.focusIndex = 0
			} else if m.focusIndex < 0 {
				m.focusIndex = len(m.inputs) + 1
			}

			cmds := make([]tea.Cmd, len(m.inputs))
			for i := 0; i <= len(m.inputs)-1; i++ {
				if i == m.focusIndex {
					cmds[i] = m.inputs[i].Focus()
					continue
				}
				m.inputs[i].Blur()
			}

			return m, tea.Batch(cmds...)
		case "enter":
			if m.focusIndex == len(m.inputs) {
				m.sslToggle = !m.sslToggle
				return m, nil
			}
			if m.focusIndex == len(m.inputs)+1 {
				return m, m.submit
			}
		case " ":
			if m.focusIndex == len(m.inputs) {
				m.sslToggle = !m.sslToggle
				return m, nil
			}
		}
	case submitMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.err = nil // Clear any previous errors
		m.database = msg.database
		return m, tea.Quit
	}

	cmd := m.updateInputs(msg)
	return m, cmd
}

func (m *formModel) updateInputs(msg tea.Msg) tea.Cmd {
	cmds := make([]tea.Cmd, len(m.inputs))

	for i := range m.inputs {
		m.inputs[i], cmds[i] = m.inputs[i].Update(msg)
	}

	return tea.Batch(cmds...)
}

func (m formModel) View() string {
	var b strings.Builder

	for i := range m.inputs {
		b.WriteString(fmt.Sprintf("%s\n", m.labels[i]))
		b.WriteString(m.inputs[i].View())
		b.WriteString("\n\n")
	}

	// Add SSL toggle
	sslToggle := "[ ] Use SSL"
	if m.sslToggle {
		sslToggle = "[x] Use SSL"
	}
	if m.focusIndex == len(m.inputs) {
		sslToggle = "> " + sslToggle
	}
	b.WriteString(sslToggle + "\n\n")

	button := "[ Submit ]"
	if m.focusIndex == len(m.inputs)+1 {
		button = "> [ " + lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("Submit") + " ]"
	} else {
		button = "  [ Submit ]"
	}
	b.WriteString(button)

	if m.err != nil {
		b.WriteString("\n\n")
		if validationErr, ok := m.err.(*api.ValidationError); ok {
			// Instead of calling PrintValidationError(), we'll format the error here
			b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render(fmt.Sprintf("Validation Error: %s", validationErr.Error())))
		} else {
			b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render(m.err.Error()))
		}
	}

	b.WriteString("\n\n" + lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		Render("(Use tab/shift+tab to navigate, CTRL+S to submit from anywhere)"))

	return b.String()
}

type submitMsg struct {
	err      error
	database *api.PostgresDatabaseCreate
}

func (m *formModel) submit() tea.Msg {
	database := &api.PostgresDatabaseCreate{
		Database: getValueOrDefault(m.inputs[0].Value(), "postgres"),
		Hostname: getValueOrDefault(m.inputs[1].Value(), "localhost"),
		Username: getValueOrDefault(m.inputs[3].Value(), "postgres"),
		Password: m.inputs[4].Value(),
		Name:     m.inputs[5].Value(),
		SSL:      m.sslToggle,
	}

	port, err := strconv.Atoi(getValueOrDefault(m.inputs[2].Value(), "5432"))
	if err != nil {
		return submitMsg{err: fmt.Errorf("invalid port number: %v", err)}
	}
	database.Port = port

	return submitMsg{database: database}
}

func (m *formModel) getActionString() string {
	if m.id != "" {
		return "update"
	}
	return "create"
}

func promptForNewDatabase(ctx *context.Context) (*api.PostgresDatabase, error) {
	newDatabase, err := promptForDatabaseForm(ctx, nil)
	if err != nil {
		return nil, err
	}

	createdDatabase, err := api.AddPostgresDatabase(ctx, newDatabase)
	if err != nil {
		return nil, fmt.Errorf("failed to create new database: %w", err)
	}

	return createdDatabase, nil
}

func promptForEditDatabase(ctx *context.Context, existingDatabase *api.PostgresDatabase) (*api.PostgresDatabaseCreate, error) {
	return promptForDatabaseForm(ctx, existingDatabase)
}

func promptForDatabaseForm(ctx *context.Context, existingDatabase *api.PostgresDatabase) (*api.PostgresDatabaseCreate, error) {
	p := tea.NewProgram(initialDatabaseModel(ctx, existingDatabase))
	m, err := p.Run()
	if err != nil {
		return nil, fmt.Errorf("error running program: %w", err)
	}

	if m, ok := m.(formModel); ok {
		if m.err != nil {
			return nil, m.err
		}
		return m.database, nil
	}

	return nil, fmt.Errorf("could not get database from model")
}

func promptForDatabase(ctx *context.Context) (string, error) {
	databases, err := api.FetchPostgresDatabases(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch databases: %w", err)
	}

	databaseOptions := make([]string, len(databases)+1)
	databaseOptions[0] = "Connect new database"
	for i, db := range databases {
		databaseOptions[i+1] = fmt.Sprintf("%s (ID: %s)", db.Name, db.ID)
	}

	var choice string
	err = survey.AskOne(&survey.Select{
		Message: "Choose a database or connect a new one:",
		Options: databaseOptions,
		Filter: func(filterValue string, optValue string, index int) bool {
			return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
		},
	}, &choice)
	if err != nil {
		return "", fmt.Errorf("failed to get user input: %w", err)
	}

	if choice == "Connect new database" {
		createdDatabase, err := promptForNewDatabase(ctx)
		if err != nil {
			return "", err
		}
		return createdDatabase.ID, nil
	}

	// Extract the database ID from the selected option
	parts := strings.Split(choice, "(ID: ")
	if len(parts) == 2 {
		return strings.TrimRight(parts[1], ")"), nil
	}
	return "", fmt.Errorf("invalid database choice format")
}

func getValueOrDefault(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}
