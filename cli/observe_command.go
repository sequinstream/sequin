package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	sequinContext "sequin-cli/context"

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
	messages  *Message
	consumers *Consumer
	err       error
	config    *Config
	activeTab int
	tabs      []string
	ctx       *sequinContext.Context
}

func initialModel(config *Config, ctx *sequinContext.Context) model {
	m := model{
		config:    config,
		activeTab: 0,
		tabs:      []string{"Messages", "Consumers"},
		messages:  NewMessage(config),
		consumers: NewConsumer(config, ctx),
		ctx:       ctx,
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

// Add this new private function
func calculateLimit() int {
	_, height, _ := term.GetSize(int(os.Stdout.Fd()))
	return height - 10 // Subtract space for headers and footers
}

func (m model) Init() tea.Cmd {
	limit := calculateLimit()

	return tea.Batch(
		doTick(),
		doSlowTick(),
		func() tea.Msg { return m.messages.FetchMessages(limit) },
		func() tea.Msg { return m.consumers.FetchConsumers(limit) },
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "tab", "right", "l":
			m.activeTab = (m.activeTab + 1) % len(m.tabs)
			return m, nil
		case "shift+tab", "left", "h":
			m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
			return m, nil
		case "m":
			m.activeTab = 0 // Switch to Messages tab
			return m, nil
		case "c":
			m.activeTab = 1 // Switch to Consumers tab
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
				// Add this line to fetch pending and next messages when toggling detail view
				return m, m.fetchPendingAndNextMessages()
			}
			return m, nil
		case "q":
			return m, tea.Quit
		case "ctrl+c":
			return m, tea.Quit
		}
	case tickMsg:
		return m, doTick()
	case slowTickMsg:
		limit := calculateLimit()
		return m, tea.Batch(
			doSlowTick(),
			func() tea.Msg { return m.messages.FetchMessages(limit) },
			func() tea.Msg { return m.consumers.FetchConsumers(limit) },
		)
	case error:
		m.err = msg
		return m, nil
	}
	return m, nil
}

// Add this new method to the model
func (m model) fetchPendingAndNextMessages() tea.Cmd {
	return func() tea.Msg {
		err := m.consumers.FetchPendingAndNextMessages()
		if err != nil {
			return err
		}
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
		content = m.messages.View(width, height-4)
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

func (m model) fetchMessages() tea.Cmd {
	return func() tea.Msg {
		limit := calculateLimit()
		err := m.messages.FetchMessages(limit)
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
