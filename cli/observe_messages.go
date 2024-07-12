package cli

import (
	"fmt"
	"strings"
	"time"

	"sequin-cli/api"
	"sequin-cli/context"
)

type Message struct {
	messages []api.Message
	config   *Config
}

func NewMessage(config *Config) *Message {
	return &Message{
		config: config,
	}
}

func (m *Message) FetchMessages() error {
	ctx, err := context.LoadContext(m.config.ContextName)
	if err != nil {
		return err
	}

	messages, err := api.ListStreamMessages(ctx, "default", 10, "seq_desc", "")
	if err != nil {
		return err
	}

	m.messages = messages
	return nil
}

func (m *Message) View(width, height int) string {
	seqWidth := 10
	keyWidth := 20
	createdWidth := 25
	dataWidth := width - seqWidth - keyWidth - createdWidth - 8

	output := fmt.Sprintf("\n%-*s | %-*s | %-*s | %-*s\n", seqWidth, "Seq", keyWidth, "Key", createdWidth, "Created", dataWidth, "Data")
	output += strings.Repeat("-", width) + "\n"

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

	for i := 0; i < height-len(m.messages)-3; i++ {
		output += "\n"
	}

	return output
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
