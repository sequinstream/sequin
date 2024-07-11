package api

import "time"

// Message represents the structure of a message returned by the API
type Message struct {
	CreatedAt time.Time `json:"inserted_at"`
	Data      string    `json:"data"`
	Seq       int       `json:"seq"`
	Subject   string    `json:"subject"`
	UpdatedAt time.Time `json:"updated_at"`
}
