package api

import (
	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type API interface {
	// Consumer methods
	FetchConsumers(ctx *context.Context, streamID string) ([]models.Consumer, error)
	FetchConsumerInfo(ctx *context.Context, streamID, consumerID string) (*models.Consumer, error)
	AddConsumer(ctx *context.Context, options ConsumerCreateOptions) (*models.Consumer, error)
	EditConsumer(ctx *context.Context, streamID, consumerID string, options ConsumerUpdateOptions) (*models.Consumer, error)
	ReceiveMessages(ctx *context.Context, streamID, consumerID string, batchSize int) ([]models.MessageWithAckID, error)
	FetchMessages(ctx *context.Context, options FetchMessagesOptions) ([]models.MessageWithInfo, error)
	AckMessage(ctx *context.Context, streamID, consumerID, ackID string) error
	NackMessage(ctx *context.Context, streamID, consumerID, ackID string) error
	RemoveConsumer(ctx *context.Context, streamID, consumerID string) error

	// Stream methods
	FetchStreams(ctx *context.Context) ([]models.Stream, error)
	FetchStreamInfo(ctx *context.Context, streamID string) (*models.Stream, error)
	AddStream(ctx *context.Context, name string, oneMessagePerKey bool) (*models.Stream, error)
	RemoveStream(ctx *context.Context, streamID string) error
	PublishMessage(ctx *context.Context, streamID, key, message string) error
	ListStreamMessages(ctx *context.Context, streamIDOrName string, limit int, sort string, keyPattern string) ([]models.Message, error)
	GetStreamMessage(ctx *context.Context, streamIDOrName, messageID string) (models.Message, error)
	FetchMessageWithConsumerInfos(ctx *context.Context, streamID, messageKey string) (*models.MessageWithConsumerInfos, error)

	// Webhook methods
	FetchWebhooks(ctx *context.Context) ([]models.Webhook, error)
	FetchWebhook(ctx *context.Context, idOrName string) (*models.Webhook, error)
	CreateWebhook(ctx *context.Context, webhookData *WebhookCreateOptions) (*models.Webhook, error)
	UpdateWebhook(ctx *context.Context, idOrName string, webhookData *WebhookCreateOptions) (*models.Webhook, error)
	DeleteWebhook(ctx *context.Context, idOrName string) error

	// Postgres Database methods
	FetchPostgresDatabases(ctx *context.Context) ([]PostgresDatabase, error)
	AddPostgresDatabase(ctx *context.Context, databaseData *PostgresDatabaseCreate) (*PostgresDatabase, error)
	SetupReplicationSlotAndPublication(ctx *context.Context, databaseID, slotName, publicationName string, tables [][]string) error
	ListSchemas(ctx *context.Context, databaseID string) ([]string, error)
	ListTables(ctx *context.Context, databaseID, schema string) ([]string, error)

	// Postgres Replication methods
	FetchPostgresReplications(ctx *context.Context) ([]PostgresReplication, error)
	FetchPostgresReplicationInfo(ctx *context.Context, replicationID string) (*PostgresReplicationWithInfo, error)
	AddPostgresReplication(ctx *context.Context, replicationData *PostgresReplicationCreate) (*PostgresReplication, error)
	DeletePostgresReplication(ctx *context.Context, replicationID string) error
}
