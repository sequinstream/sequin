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

	m := model{
		config:      config,
		activeTab:   0,
		tabs:        []string{"Messages (m)", "Consumers (c)"},
		messages:    NewMessage(config),
		consumers:   NewConsumer(config, ctx),
		ctx:         ctx,
		filterInput: ti,
	}

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
	return tea.Batch(
		doTick(),
		doSlowTick(),
		m.fetchMessages(),
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
	case error:
		return m.handleError(msg)
	}
	return m, nil
}

func (m model) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.filterMode {
		return m.handleFilterModeKeyPress(msg)
	}

	switch msg.String() {
	case "tab", "right", "l":
		m.activeTab = (m.activeTab + 1) % len(m.tabs)
	case "shift+tab", "left", "h":
		m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
	case "m":
		m.activeTab = 0
	case "c":
		m.activeTab = 1
	case "up", "k":
		m.moveCursor(-1)
	case "down", "j":
		m.moveCursor(1)
	case "enter":
		return m.handleEnter()
	case "q", "ctrl+c":
		return m, tea.Quit
	case "f":
		return m.handleFilterKey()
	}

	return m, nil
}

func (m model) handleFilterModeKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "enter", "ctrl+c":
		m.filterMode = false
		m.filterInput.Blur()
		return m, m.applyFilter()
	default:
		var cmd tea.Cmd
		m.filterInput, cmd = m.filterInput.Update(msg)
		m.filterError = ""
		return m, cmd
	}
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
	return m, m.fetchPendingAndNextMessages()
}

func (m model) handleFilterKey() (tea.Model, tea.Cmd) {
	if m.activeTab == 0 && !m.messages.IsDetailView() {
		m.filterMode = true
		m.filterInput.Focus()
	}
	return m, nil
}

func (m model) handleSlowTick() (tea.Model, tea.Cmd) {
	return m, tea.Batch(
		doSlowTick(),
		m.fetchMessages(),
		m.fetchConsumers(),
	)
}

func (m model) handleError(err error) (tea.Model, tea.Cmd) {
	if m.activeTab == 0 && m.filterMode {
		m.filterError = err.Error()
		return m, nil
	}
	m.err = err
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

	minWidth := 60
	if width < minWidth {
		return fmt.Sprintf("Error: Screen too narrow (min %d, current %d)\n\nq (quit)", minWidth, width)
	}

	tabBar := m.renderTabBar(width)
	content := m.renderContent(width, height)
	bottomBar := m.renderBottomBar(width)

	return tabBar + "\n" + content + "\n" + bottomBar
}

func (m model) renderTabBar(width int) string {
	tabContent := ""
	for i, tab := range m.tabs {
		style := lipgloss.NewStyle().Padding(0, 1)
		if i == m.activeTab {
			style = style.Bold(true).Underline(true)
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
		if !m.messages.IsDetailView() {
			filterBar := m.renderFilterBar(width)
			return filterBar + "\n" + m.messages.View(width, height-7)
		}
		return m.messages.View(width, height-4)
	}
	return m.consumers.View(width, height-4)
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

		errorMessage := m.extractErrorSummary(m.filterError)
		filterContent += "\n" + errorStyle.Render("Error: "+errorMessage)
	}

	return filterBarStyle.Render(filterContent)
}

func (m model) extractErrorSummary(errorMessage string) string {
	if strings.Contains(errorMessage, "unexpected status code: 400") {
		parts := strings.SplitN(errorMessage, "body:", 2)
		if len(parts) == 2 {
			var errorBody map[string]interface{}
			if err := json.Unmarshal([]byte(strings.TrimSpace(parts[1])), &errorBody); err == nil {
				if summary, ok := errorBody["summary"].(string); ok {
					return summary
				}
			}
		}
	}
	return errorMessage
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
