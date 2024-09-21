package api

type Client struct{}

func NewClient() *Client {
	return &Client{}
}

// Implement Consumer methods
// func (c *Client) FetchConsumers(ctx *context.Context, streamID string) ([]models.Consumer, error) {
// 	return FetchConsumers(ctx, streamID)
// }
