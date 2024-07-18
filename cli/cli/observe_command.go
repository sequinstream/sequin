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

type state struct {
	messages  *MessageState
	consumers *ConsumerState
	config    *Config
	activeTab int
	tabs      []string
	ctx       *sequinContext.Context
}

func (s *state) fetchConsumers() tea.Cmd {
	return func() tea.Msg {
		err := s.consumers.FetchConsumers(calculateLimit())
		if err != nil {
			return err
		}
		return nil
	}
}

func (s *state) fetchMessages() tea.Cmd {
	return func() tea.Msg {
		err := s.messages.FetchMessages(calculateLimit(), s.messages.filter)
		if err != nil {
			return err
		}
		return nil
	}
}

func initialState(config *Config, ctx *sequinContext.Context) state {
	s := state{
		config:    config,
		activeTab: 0,
		tabs:      []string{"Messages (m)", "Consumers (c)"},
		messages:  NewMessageState(config),
		consumers: NewConsumerState(config, ctx),
		ctx:       ctx,
	}

	go s.consumers.StartMessageUpdates(context.Background())

	return s
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

func (s state) Init() tea.Cmd {
	return tea.Batch(
		doTick(),
		doSlowTick(),
		s.messages.ApplyFilter,
		s.fetchConsumers(),
	)
}

func (s state) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return s.handleKeyPress(msg)
	case tickMsg:
		return s, doTick()
	case slowTickMsg:
		return s.handleSlowTick()
	}
	return s, nil
}

func (s state) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if s.activeTab == 0 && s.messages.filterMode {
		return s, s.messages.HandleFilterModeKeyPress(msg)
	}

	switch msg.String() {
	case "tab", "right", "l":
		s.activeTab = (s.activeTab + 1) % len(s.tabs)
		s.messages.DisableDetailView()
		s.consumers.DisableDetailView()
	case "shift+tab", "left", "h":
		s.activeTab = (s.activeTab - 1 + len(s.tabs)) % len(s.tabs)
		s.messages.DisableDetailView()
		s.consumers.DisableDetailView()
	case "m":
		s.activeTab = 0
		s.messages.DisableDetailView()
		s.consumers.DisableDetailView()
	case "c":
		s.activeTab = 1
		s.messages.DisableDetailView()
		s.consumers.DisableDetailView()
	case "up", "k":
		s.moveCursor(-1)
	case "down", "j":
		s.moveCursor(1)
	case "enter":
		return s.handleEnter()
	case "q", "ctrl+c":
		return s, tea.Quit
	case "f":
		if s.activeTab == 0 {
			s.messages.HandleFilterKey()
		}
		return s, nil
	}

	return s, nil
}

func (s *state) moveCursor(direction int) {
	if s.activeTab == 0 {
		s.messages.MoveCursor(direction)
	} else {
		s.consumers.MoveCursor(direction)
	}
}

func (s state) handleEnter() (tea.Model, tea.Cmd) {
	if s.activeTab == 0 {
		s.messages.ToggleDetail()
		return s, nil
	}
	s.consumers.ToggleDetail()
	return s, s.fetchPendingAndUpcomingMessages()
}

func (s state) handleSlowTick() (tea.Model, tea.Cmd) {
	return s, tea.Batch(
		doSlowTick(),
		s.fetchMessages(),
		s.fetchConsumers(),
	)
}

func (s state) fetchPendingAndUpcomingMessages() tea.Cmd {
	return func() tea.Msg {
		err := s.consumers.fetchPendingAndUpcomingMessages()
		if err != nil {
			return err
		}
		return nil
	}
}

func (s state) View() string {
	width, height, _ := term.GetSize(int(os.Stdout.Fd()))

	minWidth := 60
	if width < minWidth {
		return fmt.Sprintf("Error: Screen too narrow (min %d, current %d)\n\nq (quit)", minWidth, width)
	}

	tabBar := s.renderTabBar(width)
	content := s.renderContent(width, height-3) // Adjust height for tab and bottom bars
	content = s.truncateOrPadContent(content, width, height-3)
	bottomBar := s.renderBottomBar(width)

	return tabBar + "\n" + content + "\n" + bottomBar
}

func (s state) truncateOrPadContent(content string, width, height int) string {
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

func (s state) renderTabBar(width int) string {
	tabContent := ""
	for i, tab := range s.tabs {
		style := lipgloss.NewStyle().Padding(0, 1)
		if i == s.activeTab {
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

func (s state) renderContent(width, height int) string {
	if s.activeTab == 0 {
		return s.messages.View(width, height)
	}
	return s.consumers.View(width, height)
}

func (s state) renderBottomBar(width int) string {
	return lipgloss.NewStyle().
		Background(lipgloss.Color("240")).
		Foreground(lipgloss.Color("255")).
		Padding(0, 1).
		Width(width).
		Align(lipgloss.Center).
		Render("q (quit), ←/→ (switch tabs), ↑/↓ (navigate list), enter (toggle details)")
}

func streamObserve(_ *fisk.ParseContext, config *Config, ctx *sequinContext.Context) error {
	p := tea.NewProgram(initialState(config, ctx), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %w", err)
	}
	return nil
}
