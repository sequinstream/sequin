package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	sequinContext "sequin-cli/context"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/choria-io/fisk"
	"golang.org/x/term"
)

// AddObserveCommands adds all observe-related commands to the given app
func AddObserveCommands(app *fisk.Application, config *Config) {
	app.Command("observe", "Observe stream in real-time").Alias("obs").Alias("o").Action(func(c *fisk.ParseContext) error {
		ctx, err := sequinContext.LoadContext(config.ContextName)
		if err != nil {
			return fmt.Errorf("failed to load context: %w", err)
		}
		return streamObserve(c, config, ctx)
	})
}

type model struct {
	messages    *Message
	consumers   *Consumer
	err         error
	config      *Config
	activeTab   int
	tabs        []string
	ctx         *sequinContext.Context
	filterInput textinput.Model
	filterMode  bool
	filterError string
}

func initialModel(config *Config, ctx *sequinContext.Context) model {
	ti := textinput.New()
	ti.Placeholder = "Filter"
	ti.CharLimit = 100
	ti.Width = 30
	ti.SetValue("")

	m := model{
		config:      config,
		activeTab:   0,
		tabs:        []string{"Messages (m)", "Consumers (c)"},
		messages:    NewMessage(config),
		consumers:   NewConsumer(config, ctx),
		ctx:         ctx,
		filterInput: ti,
		filterMode:  false,
		filterError: "",
	}

	// Start message updates
	go m.consumers.StartMessageUpdates(context.Background())

	return m
}

type tickMsg time.Time

func doTick() tea.Cmd {
	return tea.Tick(time.Millisecond*50, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func doSlowTick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return slowTickMsg(t)
	})
}

type slowTickMsg time.Time

func calculateLimit() int {
	_, height, _ := term.GetSize(int(os.Stdout.Fd()))
	return height - 10
}

func (m model) Init() tea.Cmd {
	limit := calculateLimit()

	return tea.Batch(
		doTick(),
		doSlowTick(),
		func() tea.Msg { return m.messages.FetchMessages(limit, m.filterInput.Value()) },
		func() tea.Msg { return m.consumers.FetchConsumers(limit) },
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.filterMode {
			switch msg.String() {
			case "esc", "enter", "ctrl+c":
				m.filterMode = false
				m.filterInput.Blur()
				return m, m.applyFilter()
			default:
				var cmd tea.Cmd
				m.filterInput, cmd = m.filterInput.Update(msg)
				m.filterError = "" // Clear the error when the input changes
				return m, cmd
			}
		}

		switch msg.String() {
		case "tab", "right", "l":
			m.activeTab = (m.activeTab + 1) % len(m.tabs)
			return m, nil
		case "shift+tab", "left", "h":
			m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
			return m, nil
		case "m":
			m.activeTab = 0
			return m, nil
		case "c":
			m.activeTab = 1
			return m, nil
		case "up", "k":
			if m.activeTab == 0 {
				m.messages.MoveCursor(-1)
			} else {
				m.consumers.MoveCursor(-1)
			}
			return m, nil
		case "down", "j":
			if m.activeTab == 0 {
				m.messages.MoveCursor(1)
			} else {
				m.consumers.MoveCursor(1)
			}
			return m, nil
		case "enter":
			if m.activeTab == 0 {
				m.messages.ToggleDetail()
			} else {
				m.consumers.ToggleDetail()
				return m, m.fetchPendingAndNextMessages()
			}
			return m, nil
		case "q":
			return m, tea.Quit
		case "ctrl+c":
			return m, tea.Quit
		case "f":
			if m.activeTab == 0 && !m.messages.IsDetailView() {
				m.filterMode = true
				m.filterInput.Focus()
				return m, nil
			}
		}
	case tickMsg:
		return m, doTick()
	case slowTickMsg:
		limit := calculateLimit()
		return m, tea.Batch(
			doSlowTick(),
			func() tea.Msg { return m.messages.FetchMessages(limit, m.filterInput.Value()) },
			func() tea.Msg { return m.consumers.FetchConsumers(limit) },
		)
	case error:
		if m.activeTab == 0 && m.filterMode {
			m.filterError = msg.Error()
			return m, nil
		}
		m.err = msg
		return m, nil
	}
	return m, nil
}

func (m model) fetchPendingAndNextMessages() tea.Cmd {
	return func() tea.Msg {
		err := m.consumers.FetchPendingAndNextMessages()
		if err != nil {
			return err
		}
		return nil
	}
}

func (m model) applyFilter() tea.Cmd {
	return func() tea.Msg {
		filter := m.filterInput.Value()
		if filter == "" {
			filter = ">"
		}
		limit := calculateLimit()
		err := m.messages.FetchMessages(limit, filter)
		if err != nil {
			return err
		}
		m.filterError = ""
		return nil
	}
}

func (m model) View() string {
	if m.err != nil {
		return fmt.Sprintf("Error: %v\n\nq (quit)", m.err)
	}

	width, height, _ := term.GetSize(int(os.Stdout.Fd()))

	tabContent := ""
	for i, tab := range m.tabs {
		style := lipgloss.NewStyle().Padding(0, 1)
		if i == m.activeTab {
			style = style.Bold(true).Underline(true)
		}
		tabContent += style.Render(tab) + " "
	}
	tabBar := lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderBottom(true).
		Width(width).
		Render(tabContent)

	var content string
	if m.activeTab == 0 {
		if !m.messages.IsDetailView() {
			filterBar := m.renderFilterBar(width)
			content = filterBar + "\n" + m.messages.View(width, height-7)
		} else {
			content = m.messages.View(width, height-4)
		}
	} else {
		content = m.consumers.View(width, height-4)
	}

	bottomBar := lipgloss.NewStyle().
		Background(lipgloss.Color("240")).
		Foreground(lipgloss.Color("255")).
		Padding(0, 1).
		Width(width).
		Align(lipgloss.Center).
		Render("q (quit), ←/→ (switch tabs), ↑/↓ (navigate list), enter (toggle details)")

	return tabBar + "\n" + content + "\n" + bottomBar
}

func (m model) renderFilterBar(width int) string {
	filterBarStyle := lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderBottom(true).
		Width(width)

	filterContent := "Filter (f): " + strings.TrimPrefix(m.filterInput.View(), "> ")
	if m.filterError != "" {
		errorStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("231")).
			Background(lipgloss.Color("203"))

		// Extract summary from 400 errors
		errorMessage := m.filterError
		if strings.Contains(errorMessage, "unexpected status code: 400") {
			parts := strings.SplitN(errorMessage, "body:", 2)
			if len(parts) == 2 {
				var errorBody map[string]interface{}
				if err := json.Unmarshal([]byte(strings.TrimSpace(parts[1])), &errorBody); err == nil {
					if summary, ok := errorBody["summary"].(string); ok {
						errorMessage = summary
					}
				}
			}
		}

		filterContent += "\n" + errorStyle.Render("Error: "+errorMessage)
	}

	return filterBarStyle.Render(filterContent)
}

func (m model) fetchMessages() tea.Cmd {
	return func() tea.Msg {
		limit := calculateLimit()
		err := m.messages.FetchMessages(limit, m.filterInput.Value())
		if err != nil {
			return err
		}
		return nil
	}
}

func (m model) fetchConsumers() tea.Cmd {
	return func() tea.Msg {
		limit := calculateLimit()
		err := m.consumers.FetchConsumers(limit)
		if err != nil {
			return err
		}
		return nil
	}
}

func streamObserve(_ *fisk.ParseContext, config *Config, ctx *sequinContext.Context) error {
	p := tea.NewProgram(initialModel(config, ctx), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %w", err)
	}
	return nil
}
