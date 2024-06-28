package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/choria-io/fisk"
)

// StreamResponse represents the structure of the API response
type StreamResponse struct {
	Streams []Stream `json:"streams"`
}

// Stream represents the structure of a stream returned by the API
type Stream struct {
	ID            string    `json:"id"`
	Idx           int       `json:"idx"`
	ConsumerCount int       `json:"consumer_count"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	MessageCount  int       `json:"message_count"`
}

// AddStreamCommands adds all stream-related commands to the given app
func AddStreamCommands(app *fisk.Application) {
	streamCmd := app.Command("stream", "Stream related commands")

	streamCmd.Command("ls", "List streams").Action(func(c *fisk.ParseContext) error {
		return streamLs(c)
	})
}

func streamLs(_ *fisk.ParseContext) error {
	resp, err := http.Get("http://localhost:4000/api/streams")
	if err != nil {
		return fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %w", err)
	}

	var streamResponse StreamResponse
	err = json.Unmarshal(body, &streamResponse)
	if err != nil {
		return fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	// Create a new tabwriter
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Print header
	fmt.Fprintln(w, "ID\tIndex\tConsumers\tMessages\tCreated At\tUpdated At")
	fmt.Fprintln(w, "--\t-----\t---------\t--------\t----------\t----------")

	// Print each stream
	for _, s := range streamResponse.Streams {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%s\t%s\n",
			s.ID, s.Idx, s.ConsumerCount, s.MessageCount,
			s.CreatedAt.Format(time.RFC3339),
			s.UpdatedAt.Format(time.RFC3339))
	}

	// Flush the tabwriter
	w.Flush()

	return nil
}
