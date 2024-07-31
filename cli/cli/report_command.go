package cli

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/choria-io/fisk"
	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/constants"
)

func AddReportCommand(app *fisk.Application, _ *Config) {
	app.Command("report", "Report a bug or submit a feature request").Action(runReportCommand)
}

func runReportCommand(_ *fisk.ParseContext) error {
	reportType := ""
	prompt := &survey.Select{
		Message: "What would you like to report?",
		Options: []string{"🐛 Bug", "✨ Request"},
	}
	err := survey.AskOne(prompt, &reportType)
	if err != nil {
		return err
	}

	switch reportType {
	case "🐛 Bug":
		return runBugReportCommand(nil)
	case "✨ Request":
		return runRequestCommand(nil)
	default:
		return fmt.Errorf("invalid report type")
	}
}

func runBugReportCommand(_ *fisk.ParseContext) error {
	p := tea.NewProgram(initialBugModel())
	_, err := p.Run()
	return err
}

func runRequestCommand(_ *fisk.ParseContext) error {
	p := tea.NewProgram(initialRequestModel())
	_, err := p.Run()
	return err
}

type reportModel struct {
	description      textarea.Model
	stepsToReproduce textarea.Model
	submitButton     string
	focusIndex       int
}

func initialBugModel() reportModel {
	m := reportModel{
		description:      textarea.New(),
		stepsToReproduce: textarea.New(),
		submitButton:     "[ Submit ]",
		focusIndex:       0,
	}
	m.description.Placeholder = "Description of the bug"
	m.stepsToReproduce.Placeholder = "Steps to reproduce"
	m.description.Focus()
	return m
}

func initialRequestModel() requestModel {
	m := requestModel{
		description:  textarea.New(),
		useCase:      textarea.New(),
		submitButton: "[ Submit ]",
		focusIndex:   0,
	}
	m.description.Placeholder = "Request description"
	m.useCase.Placeholder = "What is your use case"
	m.description.Focus()
	return m
}

func (m reportModel) Init() tea.Cmd {
	return nil
}

func (m reportModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "tab", "shift+tab":
			m.focusIndex = (m.focusIndex + 1) % 3
			m.description.Blur()
			m.stepsToReproduce.Blur()
			if m.focusIndex == 0 {
				m.description.Focus()
			} else if m.focusIndex == 1 {
				m.stepsToReproduce.Focus()
			}
		case "enter":
			if m.focusIndex == 2 {
				return m, tea.Sequence(
					submitReport(m.description.Value(), m.stepsToReproduce.Value()),
					tea.Quit,
				)
			}
		}
	}

	if m.focusIndex == 0 {
		m.description, cmd = m.description.Update(msg)
	} else if m.focusIndex == 1 {
		m.stepsToReproduce, cmd = m.stepsToReproduce.Update(msg)
	}
	return m, cmd
}

func (m reportModel) View() string {
	submitButton := m.submitButton
	if m.focusIndex == 2 {
		submitButton = "> " + m.submitButton + " <"
	}
	return fmt.Sprintf(
		"Report a Bug\n\nDescription:\n%s\n\nSteps to Reproduce:\n%s\n\n%s\n\nPress Tab to switch fields.",
		m.description.View(),
		m.stepsToReproduce.View(),
		submitButton,
	)
}

func submitReport(description, stepsToReproduce string) tea.Cmd {
	return func() tea.Msg {
		logContent, err := getLastNLines(constants.LogFilePath, 25)
		if err != nil {
			return fmt.Errorf("error reading log file: %v", err)
		}

		err = api.ReportBug(description, stepsToReproduce, logContent)
		if err != nil {
			log.Printf("error submitting report: %v", err)
			return fmt.Errorf("error submitting report: %v", err)
		}

		fmt.Println("\n🎉 Thank you for submitting a bug report! Your feedback helps us improve Sequin.")
		return nil
	}
}

type requestModel struct {
	description  textarea.Model
	useCase      textarea.Model
	submitButton string
	focusIndex   int
}

func (m requestModel) Init() tea.Cmd {
	return nil
}

func (m requestModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "tab", "shift+tab":
			m.focusIndex = (m.focusIndex + 1) % 3
			m.description.Blur()
			m.useCase.Blur()
			if m.focusIndex == 0 {
				m.description.Focus()
			} else if m.focusIndex == 1 {
				m.useCase.Focus()
			}
		case "enter":
			if m.focusIndex == 2 {
				return m, tea.Sequence(
					submitRequest(m.description.Value(), m.useCase.Value()),
					tea.Quit,
				)
			}
		}
	}

	if m.focusIndex == 0 {
		m.description, cmd = m.description.Update(msg)
	} else if m.focusIndex == 1 {
		m.useCase, cmd = m.useCase.Update(msg)
	}
	return m, cmd
}

func (m requestModel) View() string {
	submitButton := m.submitButton
	if m.focusIndex == 2 {
		submitButton = "> " + m.submitButton + " <"
	}
	return fmt.Sprintf(
		"Submit a Feature Request\n\nRequest Description:\n%s\n\nUse Case:\n%s\n\n%s\n\nPress Tab to switch fields, Enter to submit.",
		m.description.View(),
		m.useCase.View(),
		submitButton,
	)
}

func submitRequest(description, useCase string) tea.Cmd {
	return func() tea.Msg {
		logContent, err := getLastNLines(constants.LogFilePath, 25)
		if err != nil {
			return fmt.Errorf("error reading log file: %v", err)
		}

		err = api.ReportRequest(description, useCase, logContent)
		if err != nil {
			return fmt.Errorf("error submitting request: %v", err)
		}

		fmt.Println("\n🌟 Thank you for submitting a feature request! We appreciate your input in shaping Sequin's future.")
		return nil
	}
}

func getLastNLines(filePath string, n int) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	lines := make([]string, n)
	i := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if i < n {
			lines[i] = scanner.Text()
			i++
		} else {
			lines = append(lines[1:], scanner.Text())
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return strings.Join(lines, "\n"), nil
}
