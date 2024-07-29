package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sequinstream/sequin/cli/api"
	sequinContext "github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/choria-io/fisk"
	"golang.org/x/term"
)

// AddObserveCommands adds all observe-related commands to the given app
func AddObserveCommands(app *fisk.Application, config *Config) {
	observeCmd := app.Command("observe", "Observe stream in real-time").
		Alias("obs").
		Alias("o")

	listenFlag := observeCmd.Flag("listen", "Listen for incoming messages").
		Short('l').
		Bool()

	observeCmd.Action(func(c *fisk.ParseContext) error {
		ctx, err := sequinContext.LoadContext(config.ContextName)
		if err != nil {
			return fmt.Errorf("failed to load context: %w", err)
		}

		if *listenFlag {
			return streamListen(c, config, ctx)
		}

		return streamObserve(c, config, ctx)
	})
}

type TabType int

const (
	StreamsTab TabType = iota
	MessagesTab
	ConsumersTab
)

type state struct {
	messages       *MessageState
	consumers      *ConsumerState
	streams        *StreamState
	config         *Config
	activeTab      TabType
	ctx            *sequinContext.Context
	selectedStream *models.Stream
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
		activeTab:      StreamsTab,
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
	return tea.Tick(time.Second*10, func(t time.Time) tea.Msg {
		return slowTickMsg(t)
	})
}

type slowTickMsg time.Time

func calculateLimit() int {
	_, height, _ := term.GetSize(int(os.Stdout.Fd()))
	return height - 9
}

func (s *state) Init() tea.Cmd {
	return tea.Batch(
		doTick(),
		doSlowTick(),
		s.messages.ApplyFilter,
		s.fetchConsumers(),
		s.fetchStreams(),
	)
}

func (s *state) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return s.handleKeyPress(msg)
	case tickMsg:
		return s, doTick()
	case slowTickMsg:
		return s.handleSlowTick()
	default:
		return s, nil
	}
}

func (s *state) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if s.activeTab == MessagesTab && s.messages.filterMode {
		return s, s.messages.HandleFilterModeKeyPress(msg)
	}

	switch msg.String() {
	case "tab", "right", "l":
		if s.selectedStream != nil {
			s.setActiveTab(s.nextTab())
		}
	case "shift+tab", "left", "h":
		if s.selectedStream != nil {
			s.setActiveTab(s.previousTab())
		}
	case "m":
		if s.selectedStream != nil {
			s.setActiveTab(MessagesTab)
		}
	case "c":
		if s.selectedStream != nil {
			s.setActiveTab(ConsumersTab)
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
		if s.activeTab == MessagesTab {
			s.messages.HandleFilterKey()
		}
		return s, nil
	}

	return s, nil
}

// Helper function to get the next tab
func (s *state) nextTab() TabType {
	switch s.activeTab {
	case StreamsTab:
		return StreamsTab
	case MessagesTab:
		return ConsumersTab
	case ConsumersTab:
		return MessagesTab
	default:
		return StreamsTab
	}
}

// Helper function to get the previous tab
func (s *state) previousTab() TabType {
	switch s.activeTab {
	case StreamsTab:
		return StreamsTab
	case MessagesTab:
		return ConsumersTab
	case ConsumersTab:
		return MessagesTab
	default:
		return StreamsTab
	}
}

func (s *state) handleBackspace() (tea.Model, tea.Cmd) {
	switch s.activeTab {
	case StreamsTab:
		// No change needed here
	case MessagesTab:
		if s.messages.showDetail {
			s.messages.DisableDetailView()
		} else {
			s.setActiveTab(StreamsTab)
		}
	case ConsumersTab:
		if s.consumers.showDetail {
			s.consumers.DisableDetailView()
		} else {
			s.setActiveTab(StreamsTab)
		}
	}
	return s, nil
}

func (s *state) moveCursor(direction int) {
	switch s.activeTab {
	case StreamsTab:
		s.streams.MoveCursor(direction)
	case MessagesTab:
		s.messages.MoveCursor(direction)
	case ConsumersTab:
		s.consumers.MoveCursor(direction)
	}
}

func (s *state) handleEnter() (tea.Model, tea.Cmd) {
	switch s.activeTab {
	case StreamsTab:
		s.selectedStream = s.streams.GetSelectedStream()
		if s.selectedStream != nil {
			s.messages.SetStreamName(s.selectedStream.Name)
			s.consumers.SetStreamName(s.selectedStream.Name)
			s.setActiveTab(MessagesTab)
			return s, s.messages.ApplyFilter
		}
	case MessagesTab:
		s.messages.ToggleDetail()
	case ConsumersTab:
		s.consumers.ToggleDetail()
		return s, s.fetchPendingAndUpcomingMessages()
	}
	return s, nil
}

func (s *state) handleSlowTick() (tea.Model, tea.Cmd) {
	return s, tea.Batch(
		doSlowTick(),
		s.fetchMessages(),
		s.fetchConsumers(),
		s.fetchStreams(),
	)
}

func (s *state) fetchPendingAndUpcomingMessages() tea.Cmd {
	return func() tea.Msg {
		err := s.consumers.fetchPendingAndUpcomingMessages()
		if err != nil {
			return err
		}
		return nil
	}
}

const (
	colorLightBlue = lipgloss.Color("117")
	colorBlack     = lipgloss.Color("0")
	colorWhite     = lipgloss.Color("255")
	colorGray      = lipgloss.Color("240")
	colorGreen     = "2"
	colorRed       = "9"
	dateFormat     = time.RFC3339
	minWidth       = 60
)

func (s *state) View() string {
	width, height, _ := term.GetSize(int(os.Stdout.Fd()))

	if width < minWidth {
		return fmt.Sprintf("Error: Screen too narrow (min %d, current %d)\n\nq (quit)", minWidth, width)
	}

	tabBar := s.renderTabBar(width)
	content := s.renderContent(width, height-5)
	bottomBar := s.renderBottomBar(width)

	return tabBar + "\n" + content + "\n" + bottomBar
}

func (s *state) renderContent(width, height int) string {
	content := s.getContentForActiveTab(width, height)
	return s.truncateOrPadContent(content, width, height)
}

func (s *state) getContentForActiveTab(width, height int) string {
	if s.selectedStream == nil {
		return s.streams.View(width, height)
	}

	switch s.activeTab {
	case MessagesTab:
		return s.messages.View(width, height)
	case ConsumersTab:
		return s.consumers.View(width, height)
	default:
		return "Invalid tab"
	}
}

func (s *state) renderTabBar(width int) string {
	tabs := s.getTabs()
	contextTab := s.renderContextTab()

	tabsContent := lipgloss.JoinHorizontal(lipgloss.Left, tabs...)

	return lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderBottom(true).
		Width(width).
		Render(lipgloss.JoinHorizontal(lipgloss.Center,
			tabsContent,
			lipgloss.NewStyle().Width(width-lipgloss.Width(tabsContent)-lipgloss.Width(contextTab)).Render(""),
			contextTab,
		))
}

func (s *state) renderContextTab() string {
	contextName := s.ctx.Name
	if contextName == "" {
		contextName = "Default"
	}

	return lipgloss.NewStyle().
		Background(colorWhite).
		Foreground(colorBlack).
		Bold(true).
		Padding(0, 1).
		Render("Context: " + contextName)
}

func (s *state) getTabs() []string {
	var tabs []string
	if s.selectedStream == nil {
		tabs = append(tabs, s.renderTab("Streams", StreamsTab))
		return tabs
	} else {
		streamName := fmt.Sprintf("Stream: %s", s.selectedStream.Name)
		tabs = append(tabs, s.renderTab(streamName, StreamsTab))
		tabs = append(tabs, s.renderTab("Messages (m)", MessagesTab))
		tabs = append(tabs, s.renderTab("Consumers (c)", ConsumersTab))
		return tabs
	}
}

func (s *state) renderTab(text string, tabType TabType) string {
	style := lipgloss.NewStyle().Padding(0, 1)

	switch {
	case tabType == StreamsTab:
		style = style.
			Background(colorWhite).
			Foreground(colorBlack).
			Bold(true)
	case tabType == s.activeTab:
		style = style.
			Background(colorLightBlue).
			Foreground(colorBlack)
	}

	return style.Render(text)
}

func (s *state) truncateOrPadContent(content string, width, height int) string {
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

func (s *state) renderBottomBar(width int) string {
	var content string
	if s.selectedStream == nil {
		content = "q (quit), ↑/↓ (navigate list), enter (select stream)"
	} else {
		content = "q (quit), ←/→ (switch tabs), ↑/↓ (navigate list), enter (toggle details), backspace (go back)"
	}
	return lipgloss.NewStyle().
		Background(colorGray).
		Foreground(colorWhite).
		Padding(0, 1).
		Width(width).
		Align(lipgloss.Center).
		Render(content)
}

func streamObserve(_ *fisk.ParseContext, config *Config, ctx *sequinContext.Context) error {
	statePtr := &state{}                  // Create a pointer to the state
	*statePtr = initialState(config, ctx) // Initialize the state

	observeChannel, err := api.NewObserveChannel(ctx)
	if err != nil {
		return fmt.Errorf("failed to create observe channel: %w", err)
	}

	err = observeChannel.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to observe channel: %w", err)
	}

	statePtr.registerCallbacks(observeChannel)

	p := tea.NewProgram(statePtr, tea.WithAltScreen()) // Pass the pointer to the program
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %w", err)
	}
	return nil
}

func (s *state) registerCallbacks(observeChannel *api.ObserveChannel) {
	observeChannel.OnStreamCreated(func(stream models.Stream) {
		s.streams.FetchStreams(calculateLimit())
	})

	observeChannel.OnStreamUpdated(func(stream models.Stream) {
		s.streams.FetchStreams(calculateLimit())
	})

	observeChannel.OnStreamDeleted(func(stream models.Stream) {
		if s.selectedStream != nil && s.selectedStream.Name == stream.Name {
			s.setActiveTab(StreamsTab)
		}

		s.streams.FetchStreams(calculateLimit())
	})

	observeChannel.OnConsumerCreated(func(consumer models.Consumer) {
		s.consumers.FetchConsumers(calculateLimit())
	})

	observeChannel.OnConsumerUpdated(func(consumer models.Consumer) {
		s.consumers.FetchConsumers(calculateLimit())
	})

	observeChannel.OnConsumerDeleted(func(consumer models.Consumer) {
		s.consumers.FetchConsumers(calculateLimit())
	})

	observeChannel.OnMessagesUpserted(func(messages []models.Message) {
		if s.selectedStream != nil {
			filteredMessages := filterMessagesForStream(messages, s.selectedStream.ID)
			s.messages.MessagesUpserted(filteredMessages, calculateLimit())
		}
	})
}

func filterMessagesForStream(messages []models.Message, streamID string) []models.Message {
	filtered := make([]models.Message, 0)
	for _, msg := range messages {
		if msg.StreamID == streamID {
			filtered = append(filtered, msg)
		}
	}
	return filtered
}

func streamListen(_ *fisk.ParseContext, _ *Config, ctx *sequinContext.Context) error {
	observeChannel, err := api.NewObserveChannel(ctx)
	if err != nil {
		return fmt.Errorf("failed to create observe channel: %w", err)
	}

	err = observeChannel.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to observe channel: %w", err)
	}

	fmt.Println("Listening for incoming messages. Press Ctrl+C to exit.")

	observeChannel.OnMessagesUpserted(func(messages []models.Message) {
		for _, msg := range messages {
			fmt.Printf("New message: Stream ID: %s, Sequence: %d, Data: %s\n", msg.StreamID, msg.Seq, msg.Data)
		}
	})

	// Keep the program running
	select {}
}

// Add this new function
func (s *state) setActiveTab(tab TabType) {
	s.activeTab = tab
	s.messages.DisableDetailView()
	s.consumers.DisableDetailView()

	switch tab {
	case StreamsTab:
		s.selectedStream = nil
		s.messages.SetStreamName("")
		s.consumers.SetStreamName("")
	case ConsumersTab:
		s.consumers.WillAppear(calculateLimit())
	default:
		// No change needed here
	}
}
