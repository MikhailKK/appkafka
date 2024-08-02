package http

import (
	"encoding/json"
	"net/http"

	"github.com/MikhailKK/appkafka/app"
)

func StartHTTPServer() {
	http.HandleFunc("/last-message", getLastMessageHandler)
	http.ListenAndServe(":8080", nil)
}

func getLastMessageHandler(w http.ResponseWriter, r *http.Request) {
	msg := app.GetLastMessage()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msg)
}
