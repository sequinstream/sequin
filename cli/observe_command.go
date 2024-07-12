package cli

import (
	"fmt"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/choria-io/fisk"
	"golang.org/x/term"
)

// AddObserveCommands adds all observe-related commands to the given app
func AddObserveCommands(app *fisk.Application, config *Config) {
	app.Command("observe", "Observe stream in real-time").Alias("obs").Alias("o").Action(func(c *fisk.ParseContext) error {
		return streamObserve(c, config)
	})
}

type model struct {
	messages  *Message
	consumers *Consumer
	err       error
	config    *Config
	activeTab int
	tabs      []string
}

func initialModel(config *Config) model {
	return model{
		config:    config,
		activeTab: 0,
		tabs:      []string{"Messages", "Consumers"},
		messages:  NewMessage(config),
		consumers: NewConsumer(config),
	}
}

type tickMsg time.Time

func doTick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		doTick(),
		func() tea.Msg { return m.messages.FetchMessages() },
		func() tea.Msg { return m.consumers.FetchConsumers() },
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
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
		}
	case tickMsg:
		return m, tea.Batch(
			doTick(),
			func() tea.Msg { return m.messages.FetchMessages() },
			func() tea.Msg { return m.consumers.FetchConsumers() },
		)
	case error:
		m.err = msg
		return m, nil
	}
	return m, nil
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
		content = m.messages.View(width, height-3)
	} else {
		content = m.consumers.View(width, height-3)
	}

	bottomBar := lipgloss.NewStyle().
		Background(lipgloss.Color("240")).
		Foreground(lipgloss.Color("255")).
		Padding(0, 1).
		Width(width).
		Align(lipgloss.Center).
		Render("q (quit), ←/→ (switch tabs), m (Messages), c (Consumers)")

	return tabBar + "\n" + content + bottomBar
}

func (m model) fetchMessages() tea.Cmd {
	return func() tea.Msg {
		err := m.messages.FetchMessages()
		if err != nil {
			return err
		}
		return nil
	}
}

func (m model) fetchConsumers() tea.Cmd {
	return func() tea.Msg {
		err := m.consumers.FetchConsumers()
		if err != nil {
			return err
		}
		return nil
	}
}

func streamObserve(_ *fisk.ParseContext, config *Config) error {
	p := tea.NewProgram(initialModel(config), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %w", err)
	}
	return nil
}
