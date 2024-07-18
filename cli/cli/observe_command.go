package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	sequinContext "github.com/sequinstream/sequin/cli/context"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/choria-io/fisk"
	"golang.org/x/term"
)

// AddObserveCommands adds all observe-related commands to the given app
func AddObserveCommands(app *fisk.Application, config *Config) {
	app.Command("observe", "Observe stream in real-time").
		Alias("obs").
		Alias("o").
		Action(func(c *fisk.ParseContext) error {
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
	config    *Config
	activeTab int
	tabs      []string
	ctx       *sequinContext.Context
}

// Add these methods to the model struct

func (m *model) fetchConsumers() tea.Cmd {
	return func() tea.Msg {
		err := m.consumers.FetchConsumers(calculateLimit())
		if err != nil {
			return err
		}
		return nil
	}
}

func (m *model) fetchMessages() tea.Cmd {
	return func() tea.Msg {
		err := m.messages.FetchMessages(calculateLimit(), m.messages.filter)
		if err != nil {
			return err
		}
		return nil
	}
}

func initialModel(config *Config, ctx *sequinContext.Context) model {
	m := model{
		config:    config,
		activeTab: 0,
		tabs:      []string{"Messages (m)", "Consumers (c)"},
		messages:  NewMessage(config),
		consumers: NewConsumer(config, ctx),
		ctx:       ctx,
	}

	go m.consumers.StartMessageUpdates(context.Background())

	return m
}

func doTick() tea.Cmd {
	return tea.Tick(time.Millisecond*50, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func doSlowTick() tea.Cmd {
	return tea.Tick(time.Second/10, func(t time.Time) tea.Msg {
		return slowTickMsg(t)
	})
}

type slowTickMsg time.Time

func calculateLimit() int {
	_, height, _ := term.GetSize(int(os.Stdout.Fd()))
	return height - 9
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		doTick(),
		doSlowTick(),
		m.messages.ApplyFilter,
		m.fetchConsumers(),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKeyPress(msg)
	case tickMsg:
		return m, doTick()
	case slowTickMsg:
		return m.handleSlowTick()
	}
	return m, nil
}

func (m model) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.activeTab == 0 && m.messages.filterMode {
		return m, m.messages.HandleFilterModeKeyPress(msg)
	}

	switch msg.String() {
	case "tab", "right", "l":
		m.activeTab = (m.activeTab + 1) % len(m.tabs)
		m.messages.DisableDetailView()
		m.consumers.DisableDetailView()
	case "shift+tab", "left", "h":
		m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
		m.messages.DisableDetailView()
		m.consumers.DisableDetailView()
	case "m":
		m.activeTab = 0
		m.messages.DisableDetailView()
		m.consumers.DisableDetailView()
	case "c":
		m.activeTab = 1
		m.messages.DisableDetailView()
		m.consumers.DisableDetailView()
	case "up", "k":
		m.moveCursor(-1)
	case "down", "j":
		m.moveCursor(1)
	case "enter":
		return m.handleEnter()
	case "q", "ctrl+c":
		return m, tea.Quit
	case "f":
		if m.activeTab == 0 {
			m.messages.HandleFilterKey()
		}
		return m, nil
	}

	return m, nil
}

func (m *model) moveCursor(direction int) {
	if m.activeTab == 0 {
		m.messages.MoveCursor(direction)
	} else {
		m.consumers.MoveCursor(direction)
	}
}

func (m model) handleEnter() (tea.Model, tea.Cmd) {
	if m.activeTab == 0 {
		m.messages.ToggleDetail()
		return m, nil
	}
	m.consumers.ToggleDetail()
	return m, m.fetchPendingAndUpcomingMessages()
}

func (m model) handleSlowTick() (tea.Model, tea.Cmd) {
	return m, tea.Batch(
		doSlowTick(),
		m.fetchMessages(),
		m.fetchConsumers(),
	)
}

func (m model) fetchPendingAndUpcomingMessages() tea.Cmd {
	return func() tea.Msg {
		err := m.consumers.fetchPendingAndUpcomingMessages()
		if err != nil {
			return err
		}
		return nil
	}
}

func (m model) View() string {
	width, height, _ := term.GetSize(int(os.Stdout.Fd()))

	minWidth := 60
	if width < minWidth {
		return fmt.Sprintf("Error: Screen too narrow (min %d, current %d)\n\nq (quit)", minWidth, width)
	}

	tabBar := m.renderTabBar(width)
	content := m.renderContent(width, height-3) // Adjust height for tab and bottom bars
	content = m.truncateOrPadContent(content, width, height-3)
	bottomBar := m.renderBottomBar(width)

	return tabBar + "\n" + content + "\n" + bottomBar
}

func (m model) truncateOrPadContent(content string, width, height int) string {
	contentLines := strings.Split(content, "\n")
	if len(contentLines) > height {
		// Truncate content and add indicator
		contentLines = contentLines[:height-1]
		warningStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("3")). // Yellow text for warning
			Width(width).
			Align(lipgloss.Center)
		contentLines = append(contentLines, warningStyle.Render("▼ More content below ▼"))
	} else if len(contentLines) < height {
		// Pad content
		for i := len(contentLines); i < height; i++ {
			contentLines = append(contentLines, "")
		}
	}

	return strings.Join(contentLines, "\n")
}

func (m model) renderTabBar(width int) string {
	tabContent := ""
	for i, tab := range m.tabs {
		style := lipgloss.NewStyle().Padding(0, 1)
		if i == m.activeTab {
			style = style.
				Background(lipgloss.Color("117")). // Light blue background
				Foreground(lipgloss.Color("0"))    // Black text
		}
		tabContent += style.Render(tab) + " "
	}
	return lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderBottom(true).
		Width(width).
		Render(tabContent)
}

func (m model) renderContent(width, height int) string {
	if m.activeTab == 0 {
		return m.messages.View(width, height)
	}
	return m.consumers.View(width, height)
}

func (m model) renderBottomBar(width int) string {
	return lipgloss.NewStyle().
		Background(lipgloss.Color("240")).
		Foreground(lipgloss.Color("255")).
		Padding(0, 1).
		Width(width).
		Align(lipgloss.Center).
		Render("q (quit), ←/→ (switch tabs), ↑/↓ (navigate list), enter (toggle details)")
}

func streamObserve(_ *fisk.ParseContext, config *Config, ctx *sequinContext.Context) error {
	p := tea.NewProgram(initialModel(config, ctx), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %w", err)
	}
	return nil
}
