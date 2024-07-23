package api

import (
	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type Client struct{}

func NewClient() *Client {
	return &Client{}
}

// Implement Consumer methods
func (c *Client) FetchConsumers(ctx *context.Context, streamID string) ([]models.Consumer, error) {
	return FetchConsumers(ctx, streamID)
}

func (c *Client) FetchConsumerInfo(ctx *context.Context, streamID, consumerID string) (*models.Consumer, error) {
	return FetchConsumerInfo(ctx, streamID, consumerID)
}

func (c *Client) AddConsumer(ctx *context.Context, options ConsumerCreateOptions) (*models.Consumer, error) {
	return AddConsumer(ctx, options)
}

func (c *Client) EditConsumer(ctx *context.Context, streamID, consumerID string, options ConsumerUpdateOptions) (*models.Consumer, error) {
	return EditConsumer(ctx, streamID, consumerID, options)
}

func (c *Client) ReceiveMessages(ctx *context.Context, streamID, consumerID string, batchSize int) ([]models.MessageWithAckID, error) {
	return ReceiveMessages(ctx, streamID, consumerID, batchSize)
}

func (c *Client) FetchMessages(ctx *context.Context, options FetchMessagesOptions) ([]models.MessageWithInfo, error) {
	return FetchMessages(ctx, options)
}

func (c *Client) AckMessage(ctx *context.Context, streamID, consumerID, ackID string) error {
	return AckMessage(ctx, streamID, consumerID, ackID)
}

func (c *Client) NackMessage(ctx *context.Context, streamID, consumerID, ackID string) error {
	return NackMessage(ctx, streamID, consumerID, ackID)
}

func (c *Client) RemoveConsumer(ctx *context.Context, streamID, consumerID string) error {
	return RemoveConsumer(ctx, streamID, consumerID)
}

// Implement Stream methods
func (c *Client) FetchStreams(ctx *context.Context) ([]models.Stream, error) {
	return FetchStreams(ctx)
}

func (c *Client) FetchStreamInfo(ctx *context.Context, streamID string) (*models.Stream, error) {
	return FetchStreamInfo(ctx, streamID)
}

func (c *Client) AddStream(ctx *context.Context, name string) (*models.Stream, error) {
	return AddStream(ctx, name)
}

func (c *Client) RemoveStream(ctx *context.Context, streamID string) error {
	return RemoveStream(ctx, streamID)
}

func (c *Client) PublishMessage(ctx *context.Context, streamID, subject, message string) error {
	return PublishMessage(ctx, streamID, subject, message)
}

func (c *Client) ListStreamMessages(ctx *context.Context, streamIDOrName string, limit int, sort string, subjectPattern string) ([]models.Message, error) {
	return ListStreamMessages(ctx, streamIDOrName, limit, sort, subjectPattern)
}

func (c *Client) GetStreamMessage(ctx *context.Context, streamIDOrName, key string) (models.Message, error) {
	return GetStreamMessage(ctx, streamIDOrName, key)
}

func (c *Client) FetchMessageWithConsumerInfos(ctx *context.Context, streamID, messageKey string) (*models.MessageWithConsumerInfos, error) {
	return FetchMessageWithConsumerInfos(ctx, streamID, messageKey)
}

// Implement Webhook methods
func (c *Client) FetchWebhooks(ctx *context.Context) ([]models.Webhook, error) {
	return FetchWebhooks(ctx)
}

func (c *Client) FetchWebhook(ctx *context.Context, idOrName string) (*models.Webhook, error) {
	return FetchWebhook(ctx, idOrName)
}

func (c *Client) CreateWebhook(ctx *context.Context, webhookData *WebhookCreateOptions) (*models.Webhook, error) {
	return CreateWebhook(ctx, webhookData)
}

func (c *Client) UpdateWebhook(ctx *context.Context, idOrName string, webhookData *WebhookCreateOptions) (*models.Webhook, error) {
	return UpdateWebhook(ctx, idOrName, webhookData)
}

func (c *Client) DeleteWebhook(ctx *context.Context, idOrName string) error {
	return DeleteWebhook(ctx, idOrName)
}

// Implement Postgres Database methods
func (c *Client) FetchPostgresDatabases(ctx *context.Context) ([]PostgresDatabase, error) {
	return FetchPostgresDatabases(ctx)
}

func (c *Client) AddPostgresDatabase(ctx *context.Context, databaseData *PostgresDatabaseCreate) (*PostgresDatabase, error) {
	return AddPostgresDatabase(ctx, databaseData)
}

func (c *Client) SetupReplicationSlotAndPublication(ctx *context.Context, databaseID, slotName, publicationName string, tables [][]string) error {
	return SetupReplicationSlotAndPublication(ctx, databaseID, slotName, publicationName, tables)
}

func (c *Client) ListSchemas(ctx *context.Context, databaseID string) ([]string, error) {
	return ListSchemas(ctx, databaseID)
}

func (c *Client) ListTables(ctx *context.Context, databaseID, schema string) ([]string, error) {
	return ListTables(ctx, databaseID, schema)
}

// Implement Postgres Replication methods
func (c *Client) FetchPostgresReplications(ctx *context.Context) ([]PostgresReplication, error) {
	return FetchPostgresReplications(ctx)
}

func (c *Client) FetchPostgresReplicationInfo(ctx *context.Context, replicationID string) (*PostgresReplicationWithInfo, error) {
	return FetchPostgresReplicationInfo(ctx, replicationID)
}

func (c *Client) AddPostgresReplication(ctx *context.Context, replicationData *PostgresReplicationCreate) (*PostgresReplication, error) {
	return AddPostgresReplication(ctx, replicationData)
}

func (c *Client) DeletePostgresReplication(ctx *context.Context, replicationID string) error {
	return DeletePostgresReplication(ctx, replicationID)
}
