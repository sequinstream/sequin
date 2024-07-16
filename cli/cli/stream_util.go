package cli

import (
	"fmt"
	"strings"

	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"

	"github.com/AlecAivazis/survey/v2"
)

func promptForStream(ctx *context.Context) (string, error) {
	streams, err := api.FetchStreams(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to fetch streams: %w", err)
	}

	streamOptions := make([]string, len(streams))
	for i, s := range streams {
		streamOptions[i] = fmt.Sprintf("%s (ID: %s)", s.Slug, s.ID)
	}

	var choice string
	err = survey.AskOne(&survey.Select{
		Message: "Choose a stream:",
		Options: streamOptions,
		Filter: func(filterValue string, optValue string, index int) bool {
			return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
		},
	}, &choice)
	if err != nil {
		return "", fmt.Errorf("failed to get user input: %w", err)
	}

	parts := strings.Split(choice, "(ID: ")
	if len(parts) == 2 {
		return strings.TrimRight(parts[1], ")"), nil
	}
	return "", fmt.Errorf("invalid stream choice format")
}

func promptForConsumer(ctx *context.Context, streamID string) (string, error) {
	consumers, err := api.FetchConsumers(ctx, streamID)
	if err != nil {
		return "", fmt.Errorf("failed to fetch consumers: %w", err)
	}

	consumerOptions := make([]string, len(consumers))
	for i, consumer := range consumers {
		consumerOptions[i] = fmt.Sprintf("%s (ID: %s)", consumer.Slug, consumer.ID)
	}

	var choice string
	err = survey.AskOne(&survey.Select{
		Message: "Choose a consumer:",
		Options: consumerOptions,
		Filter: func(filterValue string, optValue string, index int) bool {
			return strings.Contains(strings.ToLower(optValue), strings.ToLower(filterValue))
		},
	}, &choice)
	if err != nil {
		return "", fmt.Errorf("failed to get user input: %w", err)
	}

	parts := strings.Split(choice, "(ID: ")
	if len(parts) == 2 {
		return strings.TrimRight(parts[1], ")"), nil
	}
	return "", fmt.Errorf("invalid consumer choice format")
}
