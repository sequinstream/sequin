package api

// Message represents the structure of a message returned by the API
type Message struct {
	Subject string `json:"subject"`
	Data    string `json:"data"`
	Seq     int    `json:"seq"`
}
