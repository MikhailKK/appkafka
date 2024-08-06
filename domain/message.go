package domain

// Message represents the structure of the message to be sent and received.
type Message struct {
	ID     int    `json:"id"`
	Type   string `json:"type"`
	Amount int    `json:"amount"`
	Key    string `json:"key"`
}

type RefundMessage struct {
	ID     int `json:"id"`
	Reason int `json:"reson"`
}
