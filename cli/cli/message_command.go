package cli

import (
	"fmt"
	"strconv"
	"time"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/choria-io/fisk"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
)

type MessageConfig struct {
	Key    string
	Data   string
	Filter string
	Last   int
	First  int
	Table  bool
}

func AddMessageCommands(app *fisk.Application, config *Config) {
	message := app.Command("message", "Message related commands").Alias("msg").Alias("m")

	addCheat("message", message)

	m := &MessageConfig{}

	sendCmd := message.Command("send", "Send a message").Action(func(ctx *fisk.ParseContext) error {
		return messageSend(ctx, config, m)
	})
	sendCmd.Arg("key", "Key of the message").Required().StringVar(&m.Key)
	sendCmd.Arg("data", "Data payload of the message").Required().StringVar(&m.Data)

	message.Command("info", "Show message info").Action(func(ctx *fisk.ParseContext) error {
		return messageInfo(ctx, config)
	})

	listCmd := message.Command("list", "List messages").Alias("ls").Action(func(ctx *fisk.ParseContext) error {
		return messageList(ctx, config, m)
	})
	listCmd.Flag("filter", "Filter messages by key pattern").StringVar(&m.Filter)
	listCmd.Flag("last", "Show most recent N messages").Default("10").IntVar(&m.Last)
	listCmd.Flag("first", "Show least recent N messages").IntVar(&m.First)
	listCmd.Flag("table", "Display messages in a table format").BoolVar(&m.Table)
}

func messageSend(_ *fisk.ParseContext, config *Config, m *MessageConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	streamID, err := getFirstStream(ctx)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildPublishMessage(ctx, streamID, m.Key, m.Data)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)
		return nil
	}

	err = api.PublishMessage(ctx, streamID, m.Key, m.Data)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	fmt.Printf("Message sent with key '%s'\n", m.Key)
	return nil
}

func messageInfo(_ *fisk.ParseContext, config *Config) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	streamID, err := getFirstStream(ctx)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildFetchStreamInfo(ctx, streamID)
		if err != nil {
			return err
		}
		curlCmd, err := formatCurl(req)
		if err != nil {
			return err
		}

		fmt.Println(curlCmd)
		return nil
	}

	stream, err := api.FetchStreamInfo(ctx, streamID)
	if err != nil {
		return err
	}

	fmt.Printf("Total messages: %d\n", stream.Stats.MessageCount)
	fmt.Printf("Storage size: %s\n", formatBytes(stream.Stats.StorageSize))
	return nil
}

func messageList(_ *fisk.ParseContext, config *Config, m *MessageConfig) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	streamID, err := getFirstStream(ctx)
	if err != nil {
		return err
	}

	limit := m.Last
	sort := "seq_desc"

	if m.First > 0 {
		limit = m.First
		sort = "seq_asc"
	}

	populator := func() ([]api.Message, error) {
		return api.ListStreamMessages(ctx, streamID, limit, sort, m.Filter)
	}

	messages, err := populator()
	if err != nil {
		return fmt.Errorf("failed to list messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Println("No messages found")
		return nil
	}

	if m.Table {
		return displayMessagesAsTable(messages, populator)
	}

	return displayMessagesAsDump(messages)
}

func displayMessagesAsTable(messages []api.Message, populator func() ([]api.Message, error)) error {
	columns := []table.Column{
		{Title: "Key", Width: 20},
		{Title: "Sequence", Width: 10},
		{Title: "Created At", Width: 30},
		{Title: "Updated At", Width: 30},
	}

	rows := []table.Row{}
	for _, msg := range messages {
		rows = append(rows, table.Row{
			msg.Key,
			strconv.Itoa(msg.Seq),
			msg.CreatedAt.Format(time.RFC3339),
			msg.UpdatedAt.Format(time.RFC3339),
		})
	}

	t := NewTable(columns, rows, InteractiveTable)

	program := tea.NewProgram(newMessageListModel(t, messages, populator))
	if _, err := program.Run(); err != nil {
		return fmt.Errorf("error running program: %w", err)
	}

	return nil
}
func displayMessagesAsDump(messages []api.Message) error {
	headerStyle := lipgloss.NewStyle().
		Background(lipgloss.Color("240")).
		Foreground(lipgloss.Color("0")).
		Padding(0, 1)

	for _, msg := range messages {
		fmt.Println(headerStyle.Render(fmt.Sprintf("Key: %s", msg.Key)))
		fmt.Printf("Sequence:   %d\n", msg.Seq)
		fmt.Printf("Created At: %s\n", msg.CreatedAt.Format(time.RFC3339))
		fmt.Printf("Updated At: %s\n", msg.UpdatedAt.Format(time.RFC3339))
		fmt.Printf("Data:\n%s\n\n", msg.Data)
	}

	return nil
}

type messageListModel struct {
	table       *Table
	messages    []api.Message
	selected    int
	showMessage bool
	populator   func() ([]api.Message, error)
	isLoading   bool
}

func newMessageListModel(table *Table, messages []api.Message, populator func() ([]api.Message, error)) *messageListModel {
	return &messageListModel{
		table:       table,
		messages:    messages,
		selected:    -1,
		showMessage: false,
		populator:   populator,
		isLoading:   false,
	}
}

func (m *messageListModel) Init() tea.Cmd {
	return nil
}

func (m *messageListModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.showMessage {
			m.showMessage = false
			return m, nil
		}

		switch msg.String() {
		case "q", "ctrl+c", "esc":
			return m, tea.Quit
		case "enter":
			m.selected = m.table.table.Cursor()
			m.showMessage = true
			return m, nil
		case "r":
			m.isLoading = true
			return m, m.refresh()
		}
	case refreshCompleteMsg:
		m.isLoading = false
		return m, nil
	}

	var cmd tea.Cmd
	updatedTable, cmd := m.table.Update(msg)
	m.table = updatedTable.(*Table)
	return m, cmd
}

type refreshCompleteMsg struct{}

func (m *messageListModel) refresh() tea.Cmd {
	return func() tea.Msg {
		messages, err := m.populator()
		if err != nil {
			return tea.Quit
		}

		m.messages = messages
		rows := []table.Row{}
		for _, msg := range messages {
			rows = append(rows, table.Row{
				msg.Key,
				strconv.Itoa(msg.Seq),
				msg.CreatedAt.Format(time.RFC3339),
				msg.UpdatedAt.Format(time.RFC3339),
			})
		}
		m.table.table.SetRows(rows)
		return refreshCompleteMsg{}
	}
}

func (m *messageListModel) View() string {
	var content string
	var prompt string

	if m.showMessage && m.selected >= 0 && m.selected < len(m.messages) {
		msg := m.messages[m.selected]
		content = fmt.Sprintf(
			"Key:        %s\nSequence:   %d\nCreated At: %s\nUpdated At: %s\n\nData:\n%s",
			msg.Key,
			msg.Seq,
			msg.CreatedAt.Format(time.RFC3339),
			msg.UpdatedAt.Format(time.RFC3339),
			msg.Data,
		)
		prompt = "(Press any key to return to the list)"
	} else {
		content = m.table.View()
		if m.isLoading {
			prompt = "(Refreshing...)"
		} else {
			prompt = "(Press Enter to view message details, r to refresh, q to quit)"
		}
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		content,
		"\n",
		lipgloss.NewStyle().
			Background(lipgloss.Color("240")).
			Foreground(lipgloss.Color("15")).
			Padding(0, 1).
			Render(prompt),
	)
}
