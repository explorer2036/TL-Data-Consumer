package entity

// Header for messages from kafka
type Header struct {
	Key    string `json:"key"`
	Source string `json:"source"`
	Path   string `json:"path"`
	Time   string `json:"time"`
	Data   string `json:"data"`
}
