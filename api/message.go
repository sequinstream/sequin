package api

import "time"

// Message represents the structure of a message returned by the API
type Message struct {
	Seq       int       `json:"seq"`
	Subject   string    `json:"subject"`
	Data      string    `json:"data"`
	CreatedAt time.Time `json:"inserted_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
