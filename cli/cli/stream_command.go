package cli

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/choria-io/fisk"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type StreamConfig struct {
	Name             string
	Key              string
	Data             string
	Filter           string
	Last             int
	First            int
	Table            bool
	AsJSON           bool
	OneMessagePerKey bool
}

func AddStreamCommands(app *fisk.Application, config *Config, apiClient api.API) {
	stream := app.Command("stream", "Stream related commands").Alias("str").Alias("s")

	addCheat("stream", stream)

	s := &StreamConfig{}

	stream.Command("ls", "List streams").Action(func(c *fisk.ParseContext) error {
		return streamLs(c, config, apiClient)
	})

	infoCmd := stream.Command("info", "Show stream info").Action(func(c *fisk.ParseContext) error {
		return streamInfo(c, config, s, apiClient)
	})
	infoCmd.Arg("stream", "ID or name of the stream to show info for").StringVar(&config.StreamID)
	infoCmd.Flag("as-json", "Print stream info as JSON").BoolVar(&s.AsJSON)

	addCmd := stream.Command("add", "Add a new stream").Action(func(c *fisk.ParseContext) error {
		return streamAdd(c, config, s, apiClient)
	})
	addCmd.Arg("name", "Name of the stream to Add").StringVar(&s.Name)
	addCmd.Flag("json", "JSON string containing stream configuration").StringVar(&config.JSONInput)
	addCmd.Flag("json-file", "Path to JSON file containing stream configuration").StringVar(&config.JSONFile)
	addCmd.Flag("one-message-per-key", "Allow only one message per key").BoolVar(&s.OneMessagePerKey)

	rmCmd := stream.Command("rm", "Remove a stream").Action(func(c *fisk.ParseContext) error {
		return streamRm(c, config, apiClient)
	})
	rmCmd.Arg("stream", "ID or name of the stream to remove").StringVar(&config.StreamID)

	sendCmd := stream.Command("send", "Send a message to a stream").Action(func(c *fisk.ParseContext) error {
		return streamSend(c, config, s, apiClient)
	})
	sendCmd.Arg("stream", "ID or name of the stream to send to").Required().StringVar(&config.StreamID)
	sendCmd.Arg("key", "Key of the message").Required().StringVar(&s.Key)
	sendCmd.Arg("data", "Data payload of the message").Required().StringVar(&s.Data)

	viewCmd := stream.Command("view", "View messages in a stream").Action(func(c *fisk.ParseContext) error {
		return streamView(c, config, s, apiClient)
	})
	viewCmd.Arg("stream", "ID or name of the stream to view").StringVar(&config.StreamID)
	viewCmd.Flag("filter", "Filter messages by key pattern").StringVar(&s.Filter)
	viewCmd.Flag("last", "Show most recent N messages").Default("10").IntVar(&s.Last)
	viewCmd.Flag("first", "Show least recent N messages").IntVar(&s.First)
	viewCmd.Flag("table", "Display messages in a table format").BoolVar(&s.Table)
}

func streamLs(_ *fisk.ParseContext, config *Config, apiClient api.API) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildFetchStreams(ctx)
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

	streams, err := apiClient.FetchStreams(ctx)
	if err != nil {
		return err
	}

	if len(streams) == 0 {
		fmt.Println("No streams defined")
		return nil
	}

	columns := []table.Column{
		{Title: "ID", Width: 36},
		{Title: "Name", Width: 20},
		{Title: "One MSG/KEY", Width: 12},
		{Title: "Consumers", Width: 10},
		{Title: "Messages", Width: 10},
		{Title: "Storage Size", Width: 15},
		{Title: "Created At", Width: 30},
		{Title: "Updated At", Width: 30},
	}

	rows := []table.Row{}
	for _, s := range streams {
		rows = append(rows, table.Row{
			s.ID,
			s.Name,
			fmt.Sprintf("%t", s.OneMessagePerKey),
			fmt.Sprintf("%d", s.Stats.ConsumerCount),
			fmt.Sprintf("%d", s.Stats.MessageCount),
			formatBytes(s.Stats.StorageSize),
			s.CreatedAt.Format(time.RFC3339),
			s.UpdatedAt.Format(time.RFC3339),
		})
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Println("Streams")
	return t.Render()
}

func streamInfo(_ *fisk.ParseContext, config *Config, s *StreamConfig, apiClient api.API) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		config.StreamID = streamID
	}

	if config.AsCurl {
		req, err := api.BuildFetchStreamInfo(ctx, config.StreamID)
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

	if s.AsJSON {
		stream, err := apiClient.FetchStreamInfo(ctx, config.StreamID)
		if err != nil {
			return err
		}
		jsonData, err := json.MarshalIndent(stream, "", "  ")
		if err != nil {
			return fmt.Errorf("error marshaling stream to JSON: %w", err)
		}
		fmt.Println(string(jsonData))
		return nil
	}

	return displayStreamInfo(config, apiClient)
}

func displayStreamInfo(config *Config, apiClient api.API) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	stream, err := apiClient.FetchStreamInfo(ctx, config.StreamID)
	if err != nil {
		return err
	}

	columns := []table.Column{
		{Title: "Field", Width: 20},
		{Title: "Value", Width: 50},
	}

	rows := []table.Row{
		{"ID", stream.ID},
		{"Index", fmt.Sprintf("%d", stream.Idx)},
		{"One MSG/KEY", fmt.Sprintf("%t", stream.OneMessagePerKey)},
		{"Consumers", fmt.Sprintf("%d", stream.Stats.ConsumerCount)},
		{"Messages", fmt.Sprintf("%d", stream.Stats.MessageCount)},
		{"Storage Size", formatBytes(stream.Stats.StorageSize)},
		{"Created At", stream.CreatedAt.Format(time.RFC3339)},
		{"Updated At", stream.UpdatedAt.Format(time.RFC3339)},
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Printf("Information for Stream %s created %s\n\n", stream.Name, stream.CreatedAt.Format(time.RFC3339))
	return t.Render()
}

func streamAdd(_ *fisk.ParseContext, config *Config, s *StreamConfig, apiClient api.API) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if err := MergeJSONConfig(s, config.JSONInput, config.JSONFile); err != nil {
		return err
	}

	// Prompt for name if not provided
	if s.Name == "" {
		prompt := &survey.Input{
			Message: "Enter stream name:",
		}
		err = survey.AskOne(prompt, &s.Name)
		if err != nil {
			return err
		}
	}

	// Prompt for OneMessagePerKey if not provided
	if !s.OneMessagePerKey {
		prompt := &survey.Confirm{
			Message: "Allow only one message per key?",
			Default: false,
		}
		err = survey.AskOne(prompt, &s.OneMessagePerKey)
		if err != nil {
			return err
		}
	}

	if config.AsCurl {
		req, err := api.BuildAddStream(ctx, s.Name, s.OneMessagePerKey)
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

	// Add stream
	stream, err := apiClient.AddStream(ctx, s.Name, s.OneMessagePerKey)
	if err != nil {
		return fmt.Errorf("failed to add stream: %w", err)
	}

	columns := []table.Column{
		{Title: "Field", Width: 20},
		{Title: "Value", Width: 50},
	}

	rows := []table.Row{
		{"Name", stream.Name},
		{"Index", fmt.Sprintf("%d", stream.Idx)},
		{"One Message Per Key", fmt.Sprintf("%t", stream.OneMessagePerKey)},
		{"Consumers", fmt.Sprintf("%d", stream.Stats.ConsumerCount)},
		{"Messages", fmt.Sprintf("%d", stream.Stats.MessageCount)},
		{"Storage Size", formatBytes(stream.Stats.StorageSize)},
		{"Created At", stream.CreatedAt.Format(time.RFC3339)},
		{"Updated At", stream.UpdatedAt.Format(time.RFC3339)},
	}

	t := NewTable(columns, rows, PrintableTable)
	fmt.Printf("Stream %s created %s\n\n", stream.ID, stream.CreatedAt.Format(time.RFC3339))
	return t.Render()
}

func streamRm(_ *fisk.ParseContext, config *Config, apiClient api.API) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		config.StreamID = streamID
	}

	if config.AsCurl {
		req, err := api.BuildRemoveStream(ctx, config.StreamID)
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

	err = apiClient.RemoveStream(ctx, config.StreamID)
	if err != nil {
		return fmt.Errorf("failed to remove stream: %w", err)
	}

	fmt.Printf("Stream %s has been removed.\n", config.StreamID)
	return nil
}

func streamSend(_ *fisk.ParseContext, config *Config, s *StreamConfig, apiClient api.API) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.AsCurl {
		req, err := api.BuildPublishMessage(ctx, config.StreamID, s.Key, s.Data)
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

	err = apiClient.PublishMessage(ctx, config.StreamID, s.Key, s.Data)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	fmt.Printf("Message sent to stream '%s' with key '%s'\n", config.StreamID, s.Key)
	return nil
}

func streamView(_ *fisk.ParseContext, config *Config, s *StreamConfig, apiClient api.API) error {
	ctx, err := context.LoadContext(config.ContextName)
	if err != nil {
		return err
	}

	if config.StreamID == "" {
		streamID, err := promptForStream(ctx)
		if err != nil {
			return err
		}
		config.StreamID = streamID
	}

	limit := s.Last
	sort := "seq_desc"

	if s.First > 0 {
		limit = s.First
		sort = "seq_asc"
	}

	populator := func() ([]models.Message, error) {
		return apiClient.ListStreamMessages(ctx, config.StreamID, limit, sort, s.Filter)
	}

	messages, err := populator()
	if err != nil {
		return fmt.Errorf("failed to list messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Println("No messages found")
		return nil
	}

	if s.Table {
		return displayMessagesAsTable(messages, populator)
	}

	return displayMessagesAsDump(messages)
}

func displayMessagesAsTable(messages []models.Message, populator func() ([]models.Message, error)) error {
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
func displayMessagesAsDump(messages []models.Message) error {
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
	messages    []models.Message
	selected    int
	showMessage bool
	populator   func() ([]models.Message, error)
	isLoading   bool
}

func newMessageListModel(table *Table, messages []models.Message, populator func() ([]models.Message, error)) *messageListModel {
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
