package cli

// type MockAPIClient struct {
// 	FetchStreamsFunc                  func(ctx *context.Context) ([]models.Stream, error)
// 	FetchStreamInfoFunc               func(ctx *context.Context, streamID string) (*models.Stream, error)
// 	AddStreamFunc                     func(ctx *context.Context, name string) (*models.Stream, error)
// 	RemoveStreamFunc                  func(ctx *context.Context, streamID string) error
// 	PublishMessageFunc                func(ctx *context.Context, streamID, key, message string) error
// 	ListStreamMessagesFunc            func(ctx *context.Context, streamIDOrName string, limit int, sort string, keyPattern string) ([]models.Message, error)
// 	GetStreamMessageFunc              func(ctx *context.Context, streamIDOrName, key string) (models.Message, error)
// 	FetchMessageWithConsumerInfosFunc func(ctx *context.Context, streamID, messageKey string) (*models.MessageWithConsumerInfos, error)
// }

// func (m *MockAPIClient) FetchStreams(ctx *context.Context) ([]models.Stream, error) {
// 	return m.FetchStreamsFunc(ctx)
// }
