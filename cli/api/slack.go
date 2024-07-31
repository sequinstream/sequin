package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

func getSlackWebhookURL() string {
	return os.Getenv("SLACK_WEBHOOK_URL")
}

type SlackMessage struct {
	Blocks []Block `json:"blocks"`
}

type Block struct {
	Type string `json:"type"`
	Text *Text  `json:"text,omitempty"`
}

type Text struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func sendSlackMessage(message SlackMessage) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %v", err)
	}

	webhookURL := getSlackWebhookURL()
	if webhookURL == "" {
		return fmt.Errorf("SLACK_WEBHOOK_URL environment variable is not set")
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error sending message to Slack: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func ReportBug(description, steps, logContent string) error {
	message := SlackMessage{
		Blocks: []Block{
			{Type: "header", Text: &Text{Type: "plain_text", Text: "🐛 Bug Report"}},
			{Type: "section", Text: &Text{Type: "mrkdwn", Text: fmt.Sprintf("*Description:*\n%s", description)}},
			{Type: "section", Text: &Text{Type: "mrkdwn", Text: fmt.Sprintf("*Steps to reproduce:*\n%s", steps)}},
			{Type: "section", Text: &Text{Type: "mrkdwn", Text: fmt.Sprintf("*Log Content:*\n```\n%s\n```", logContent)}},
		},
	}
	return sendSlackMessage(message)
}

func ReportRequest(description, useCase, logContent string) error {
	message := SlackMessage{
		Blocks: []Block{
			{Type: "header", Text: &Text{Type: "plain_text", Text: "✨ Feature Request"}},
			{Type: "section", Text: &Text{Type: "mrkdwn", Text: fmt.Sprintf("*Request Description:*\n%s", description)}},
			{Type: "section", Text: &Text{Type: "mrkdwn", Text: fmt.Sprintf("*Use Case:*\n%s", useCase)}},
			{Type: "section", Text: &Text{Type: "mrkdwn", Text: fmt.Sprintf("*Log Content:*\n```\n%s\n```", logContent)}},
		},
	}
	return sendSlackMessage(message)
}
