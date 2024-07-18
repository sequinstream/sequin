package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sequinstream/sequin/cli/api"
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
	messages       *MessageState
	consumers      *ConsumerState
	streams        *StreamState
	config         *Config
	activeTab      int
	tabs           []string
	ctx            *sequinContext.Context
	selectedStream *api.Stream
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

func (s *state) fetchStreams() tea.Cmd {
	return func() tea.Msg {
		err := s.streams.FetchStreams(calculateLimit())
		if err != nil {
			return err
		}
		return nil
	}
}

func initialState(config *Config, ctx *sequinContext.Context) state {
	s := state{
		config:         config,
		activeTab:      0,
		tabs:           []string{"Streams (s)", "Messages (m)", "Consumers (c)"},
		messages:       NewMessageState(config),
		consumers:      NewConsumerState(config, ctx),
		streams:        NewStreamState(config),
		ctx:            ctx,
		selectedStream: nil,
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
		s.fetchStreams(),
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
	if s.activeTab == 1 && s.messages.filterMode {
		return s, s.messages.HandleFilterModeKeyPress(msg)
	}

	switch msg.String() {
	case "tab", "right", "l":
		if s.selectedStream != nil {
			s.activeTab = (s.activeTab+1)%2 + 1 // Cycle between 1 and 2
		}
		s.messages.DisableDetailView()
		s.consumers.DisableDetailView()
	case "shift+tab", "left", "h":
		if s.selectedStream != nil {
			s.activeTab = (s.activeTab-1+2)%2 + 1 // Cycle between 1 and 2
		}
		s.messages.DisableDetailView()
		s.consumers.DisableDetailView()
	case "m":
		if s.selectedStream != nil {
			s.activeTab = 1
			s.messages.DisableDetailView()
			s.consumers.DisableDetailView()
		}
	case "c":
		if s.selectedStream != nil {
			s.activeTab = 2
			s.messages.DisableDetailView()
			s.consumers.DisableDetailView()
		}
	case "s", "backspace":
		return s.handleBackspace()
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

func (s state) handleBackspace() (tea.Model, tea.Cmd) {
	switch s.activeTab {
	case 0:
		if s.selectedStream != nil {
			s.selectedStream = nil
			s.messages.SetStreamName("")
			s.consumers.SetStreamName("")
		}
	case 1:
		if s.messages.showDetail {
			s.messages.DisableDetailView()
		} else if s.selectedStream != nil {
			s.activeTab = 0
			s.selectedStream = nil
			s.messages.SetStreamName("")
			s.consumers.SetStreamName("")
		}
	case 2:
		if s.consumers.showDetail {
			s.consumers.DisableDetailView()
		} else if s.selectedStream != nil {
			s.activeTab = 0
			s.selectedStream = nil
			s.messages.SetStreamName("")
			s.consumers.SetStreamName("")
		}
	}
	return s, nil
}

func (s *state) moveCursor(direction int) {
	switch s.activeTab {
	case 0:
		s.streams.MoveCursor(direction)
	case 1:
		s.messages.MoveCursor(direction)
	case 2:
		s.consumers.MoveCursor(direction)
	}
}

func (s state) handleEnter() (tea.Model, tea.Cmd) {
	switch s.activeTab {
	case 0:
		s.selectedStream = s.streams.GetSelectedStream()
		if s.selectedStream != nil {
			s.messages.SetStreamName(s.selectedStream.Name)
			s.consumers.SetStreamName(s.selectedStream.Name)
			s.activeTab = 1
			return s, s.messages.ApplyFilter
		}
	case 1:
		s.messages.ToggleDetail()
	case 2:
		s.consumers.ToggleDetail()
		return s, s.fetchPendingAndUpcomingMessages()
	}
	return s, nil
}

func (s state) handleSlowTick() (tea.Model, tea.Cmd) {
	return s, tea.Batch(
		doSlowTick(),
		s.fetchMessages(),
		s.fetchConsumers(),
		s.fetchStreams(),
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
	content := s.renderContent(width, height-3)
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
	var tabs []string
	if s.selectedStream == nil {
		tabs = append(tabs, s.renderTab("Streams (s)", 0))
	} else {
		streamName := fmt.Sprintf("Stream (s): %s", s.selectedStream.Name)
		tabs = append(tabs, s.renderTab(streamName, -1))
		tabs = append(tabs, s.renderTab("Messages (m)", 1))
		tabs = append(tabs, s.renderTab("Consumers (c)", 2))
	}
	return lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderBottom(true).
		Width(width).
		Render(lipgloss.JoinHorizontal(lipgloss.Left, tabs...))
}

func (s state) renderTab(text string, index int) string {
	style := lipgloss.NewStyle().Padding(0, 1)

	if index == -1 { // Stream tab
		style = style.
			Background(lipgloss.Color("117")). // Light blue background
			Foreground(lipgloss.Color("0"))    // Black text
	} else if index == s.activeTab {
		style = style.
			Foreground(lipgloss.Color("117")). // Light blue text
			Underline(true)
	}

	return style.Render(text)
}

func (s state) renderContent(width, height int) string {
	if s.selectedStream == nil {
		return s.streams.View(width, height)
	}

	switch s.activeTab {
	case 0:
		return s.streams.View(width, height)
	case 1:
		return s.messages.View(width, height)
	case 2:
		return s.consumers.View(width, height)
	}

	return "Invalid tab"
}

func (s state) renderBottomBar(width int) string {
	var content string
	if s.selectedStream == nil {
		content = "q (quit), ↑/↓ (navigate list), enter (select stream)"
	} else {
		content = "q (quit), ←/→ (switch tabs), ↑/↓ (navigate list), enter (select/toggle details), backspace (go back)"
	}
	return lipgloss.NewStyle().
		Background(lipgloss.Color("240")).
		Foreground(lipgloss.Color("255")).
		Padding(0, 1).
		Width(width).
		Align(lipgloss.Center).
		Render(content)
}

func streamObserve(_ *fisk.ParseContext, config *Config, ctx *sequinContext.Context) error {
	p := tea.NewProgram(initialState(config, ctx), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %w", err)
	}
	return nil
}
