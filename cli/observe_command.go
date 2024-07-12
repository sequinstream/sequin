package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/choria-io/fisk"
	"golang.org/x/term"

	"sequin-cli/api"
	"sequin-cli/context"
)

// AddObserveCommands adds all observe-related commands to the given app
func AddObserveCommands(app *fisk.Application, config *Config) {
	app.Command("observe", "Observe stream in real-time").Alias("obs").Alias("o").Action(func(c *fisk.ParseContext) error {
		return streamObserve(c, config)
	})
}

type model struct {
	messages []api.Message
	err      error
	config   *Config
}

func initialModel(config *Config) model {
	return model{
		config: config,
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
		m.fetchMessages,
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		}
	case tickMsg:
		return m, tea.Batch(doTick(), m.fetchMessages)
	case []api.Message:
		m.messages = msg
		return m, nil
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

	// Get terminal width and height
	width, height, _ := term.GetSize(int(os.Stdout.Fd()))

	// Define column widths
	seqWidth := 10
	keyWidth := 20
	createdWidth := 25
	dataWidth := width - seqWidth - keyWidth - createdWidth - 8 // 8 for separators and padding

	// Create header
	output := fmt.Sprintf("\n%-*s | %-*s | %-*s | %-*s\n", seqWidth, "Seq", keyWidth, "Key", createdWidth, "Created", dataWidth, "Data")
	output += strings.Repeat("-", width) + "\n"

	// Add messages
	for _, msg := range m.messages {
		seq := fmt.Sprintf("%d", msg.Seq)
		key := truncateString(msg.Key, keyWidth)
		created := msg.CreatedAt.Format(time.RFC3339)
		data := truncateString(msg.Data, dataWidth)

		output += fmt.Sprintf("%-*s | %-*s | %-*s | %-*s\n",
			seqWidth, seq,
			keyWidth, key,
			createdWidth, created,
			dataWidth, data)
	}

	// Create a styled bottom bar
	bottomBar := lipgloss.NewStyle().
		Background(lipgloss.Color("240")).
		Foreground(lipgloss.Color("255")).
		Padding(0, 1).
		Width(width).
		Align(lipgloss.Center).
		Render("q (quit)")

	// Add empty lines to push the bottom bar to the bottom
	for i := 0; i < height-len(m.messages)-4; i++ {
		output += "\n"
	}

	// Add the bottom bar
	output += bottomBar

	return output
}

// Helper function to truncate strings
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func (m model) fetchMessages() tea.Msg {
	ctx, err := context.LoadContext(m.config.ContextName)
	if err != nil {
		return err
	}

	messages, err := api.ListStreamMessages(ctx, "default", 10, "seq_desc", "")
	if err != nil {
		return err
	}

	return messages
}

func streamObserve(_ *fisk.ParseContext, config *Config) error {
	p := tea.NewProgram(initialModel(config), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %w", err)
	}
	return nil
}
