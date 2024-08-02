package cli

import (
	"github.com/sequinstream/sequin/cli/api"
	"github.com/sequinstream/sequin/cli/context"
	"github.com/sequinstream/sequin/cli/models"
)

type MockAPIClient struct {
	FetchStreamsFunc                  func(ctx *context.Context) ([]models.Stream, error)
	FetchStreamInfoFunc               func(ctx *context.Context, streamID string) (*models.Stream, error)
	AddStreamFunc                     func(ctx *context.Context, name string, oneMessagePerKey bool) (*models.Stream, error)
	RemoveStreamFunc                  func(ctx *context.Context, streamID string) error
	PublishMessageFunc                func(ctx *context.Context, streamID, key, message string) error
	ListStreamMessagesFunc            func(ctx *context.Context, streamIDOrName string, limit int, sort string, keyPattern string) ([]models.Message, error)
	GetStreamMessageFunc              func(ctx *context.Context, streamIDOrName, key string) (models.Message, error)
	FetchMessageWithConsumerInfosFunc func(ctx *context.Context, streamID, messageKey string) (*models.MessageWithConsumerInfos, error)
}

func (m *MockAPIClient) FetchStreams(ctx *context.Context) ([]models.Stream, error) {
	return m.FetchStreamsFunc(ctx)
}

func (m *MockAPIClient) FetchStreamInfo(ctx *context.Context, streamID string) (*models.Stream, error) {
	return m.FetchStreamInfoFunc(ctx, streamID)
}

func (m *MockAPIClient) AddStream(ctx *context.Context, name string, oneMessagePerKey bool) (*models.Stream, error) {
	return m.AddStreamFunc(ctx, name, oneMessagePerKey)
}

func (m *MockAPIClient) RemoveStream(ctx *context.Context, streamID string) error {
	return m.RemoveStreamFunc(ctx, streamID)
}

func (m *MockAPIClient) PublishMessage(ctx *context.Context, streamID, key, message string) error {
	return m.PublishMessageFunc(ctx, streamID, key, message)
}

func (m *MockAPIClient) ListStreamMessages(ctx *context.Context, streamIDOrName string, limit int, sort string, keyPattern string) ([]models.Message, error) {
	return m.ListStreamMessagesFunc(ctx, streamIDOrName, limit, sort, keyPattern)
}

func (m *MockAPIClient) GetStreamMessage(ctx *context.Context, streamIDOrName, key string) (models.Message, error) {
	return m.GetStreamMessageFunc(ctx, streamIDOrName, key)
}

func (m *MockAPIClient) FetchMessageWithConsumerInfos(ctx *context.Context, streamID, messageKey string) (*models.MessageWithConsumerInfos, error) {
	return m.FetchMessageWithConsumerInfosFunc(ctx, streamID, messageKey)
}

// Implement other methods from the api.API interface with empty implementations
func (m *MockAPIClient) FetchConsumers(ctx *context.Context, streamID string) ([]models.Consumer, error) {
	return nil, nil
}

func (m *MockAPIClient) FetchConsumerInfo(ctx *context.Context, streamID, consumerID string) (*models.Consumer, error) {
	return nil, nil
}

func (m *MockAPIClient) AddConsumer(ctx *context.Context, options api.ConsumerCreateOptions) (*models.Consumer, error) {
	return nil, nil
}

func (m *MockAPIClient) EditConsumer(ctx *context.Context, streamID, consumerID string, options api.ConsumerUpdateOptions) (*models.Consumer, error) {
	return nil, nil
}

func (m *MockAPIClient) ReceiveMessages(ctx *context.Context, streamID, consumerID string, batchSize int) ([]models.MessageWithAckID, error) {
	return nil, nil
}

func (m *MockAPIClient) FetchMessages(ctx *context.Context, options api.FetchMessagesOptions) ([]models.MessageWithInfo, error) {
	return nil, nil
}

func (m *MockAPIClient) AckMessage(ctx *context.Context, streamID, consumerID, ackID string) error {
	return nil
}

func (m *MockAPIClient) NackMessage(ctx *context.Context, streamID, consumerID, ackID string) error {
	return nil
}

func (m *MockAPIClient) RemoveConsumer(ctx *context.Context, streamID, consumerID string) error {
	return nil
}

func (m *MockAPIClient) FetchWebhooks(ctx *context.Context) ([]models.Webhook, error) {
	return nil, nil
}

func (m *MockAPIClient) FetchWebhook(ctx *context.Context, idOrName string) (*models.Webhook, error) {
	return nil, nil
}

func (m *MockAPIClient) CreateWebhook(ctx *context.Context, webhookData *api.WebhookCreateOptions) (*models.Webhook, error) {
	return nil, nil
}

func (m *MockAPIClient) UpdateWebhook(ctx *context.Context, idOrName string, webhookData *api.WebhookCreateOptions) (*models.Webhook, error) {
	return nil, nil
}

func (m *MockAPIClient) DeleteWebhook(ctx *context.Context, idOrName string) error {
	return nil
}

func (m *MockAPIClient) FetchPostgresDatabases(ctx *context.Context) ([]api.PostgresDatabase, error) {
	return nil, nil
}

func (m *MockAPIClient) AddPostgresDatabase(ctx *context.Context, databaseData *api.PostgresDatabaseCreate) (*api.PostgresDatabase, error) {
	return nil, nil
}

func (m *MockAPIClient) SetupReplicationSlotAndPublication(ctx *context.Context, databaseID, slotName, publicationName string, tables [][]string) error {
	return nil
}

func (m *MockAPIClient) ListSchemas(ctx *context.Context, databaseID string) ([]string, error) {
	return nil, nil
}

func (m *MockAPIClient) ListTables(ctx *context.Context, databaseID, schema string) ([]string, error) {
	return nil, nil
}

func (m *MockAPIClient) FetchPostgresReplications(ctx *context.Context) ([]api.PostgresReplication, error) {
	return nil, nil
}

func (m *MockAPIClient) FetchPostgresReplicationInfo(ctx *context.Context, replicationID string) (*api.PostgresReplicationWithInfo, error) {
	return nil, nil
}

func (m *MockAPIClient) AddPostgresReplication(ctx *context.Context, replicationData *api.PostgresReplicationCreate) (*api.PostgresReplication, error) {
	return nil, nil
}

func (m *MockAPIClient) DeletePostgresReplication(ctx *context.Context, replicationID string) error {
	return nil
}
