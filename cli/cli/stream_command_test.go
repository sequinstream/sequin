package cli

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
	"github.com/stretchr/testify/assert"
)

func TestStreamLs(t *testing.T) {
	mockClient := &MockAPIClient{}
	config := &Config{}

	t.Run("successful stream listing", func(t *testing.T) {
		mockClient.FetchStreamsFunc = func(ctx *context.Context) ([]models.Stream, error) {
			return []models.Stream{
				{
					ID:   "stream1",
					Name: "Stream 1",
					Stats: models.StreamStats{
						ConsumerCount: 2,
						MessageCount:  100,
						StorageSize:   1024,
					},
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
			}, nil
		}

		output := captureOutput(func() {
			err := streamLs(nil, config, mockClient)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "stream1")
		assert.Contains(t, output, "Stream 1")
		assert.Contains(t, output, "2")
		assert.Contains(t, output, "100")
		assert.Contains(t, output, "1.0 KB")
	})

	t.Run("error fetching streams", func(t *testing.T) {
		mockClient.FetchStreamsFunc = func(ctx *context.Context) ([]models.Stream, error) {
			return nil, errors.New("failed to fetch streams")
		}

		err := streamLs(nil, config, mockClient)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch streams")
	})
}

func TestStreamAdd(t *testing.T) {
	mockClient := &MockAPIClient{}
	config := &Config{}
	streamConfig := &StreamConfig{Name: "new-stream", OneMessagePerKey: true}

	t.Run("successful stream addition", func(t *testing.T) {
		mockClient.AddStreamFunc = func(ctx *context.Context, name string, oneMessagePerKey bool) (*models.Stream, error) {
			return &models.Stream{
				ID:   "new-stream-id",
				Name: name,
				Stats: models.StreamStats{
					ConsumerCount: 0,
					MessageCount:  0,
					StorageSize:   0,
				},
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}, nil
		}

		output := captureOutput(func() {
			err := streamAdd(nil, config, streamConfig, mockClient)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "new-stream-id")
		assert.Contains(t, output, "new-stream")
	})

	t.Run("error adding stream", func(t *testing.T) {
		mockClient.AddStreamFunc = func(ctx *context.Context, name string, oneMessagePerKey bool) (*models.Stream, error) {
			return nil, errors.New("failed to add stream")
		}

		err := streamAdd(nil, config, streamConfig, mockClient)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to add stream")
	})
}

func TestStreamRm(t *testing.T) {
	mockClient := &MockAPIClient{}
	config := &Config{StreamID: "stream-to-remove"}

	t.Run("successful stream removal", func(t *testing.T) {
		mockClient.RemoveStreamFunc = func(ctx *context.Context, streamID string) error {
			return nil
		}

		output := captureOutput(func() {
			err := streamRm(nil, config, mockClient)
			assert.NoError(t, err)
		})

		assert.Contains(t, output, "Stream stream-to-remove has been removed")
	})

	t.Run("error removing stream", func(t *testing.T) {
		mockClient.RemoveStreamFunc = func(ctx *context.Context, streamID string) error {
			return errors.New("failed to remove stream")
		}

		err := streamRm(nil, config, mockClient)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to remove stream")
	})
}

// Helper function to capture stdout
func captureOutput(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}
