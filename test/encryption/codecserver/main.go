// Command codecserver is a demo payload codec server. For testing and the demo only.
//
// It encrypts and decrypts payloads with the demo codec (test/encryption) under the passphrase in
// DEMO_ENCRYPTION_KEY. Point the connector programs at it with
// TEMPORAL_CODEC_ENDPOINT=http://localhost:8089, and the dev server's UI with
// `temporal server start-dev --ui-codec-endpoint http://localhost:8089`, so the UI can show
// payloads decrypted.
//
// With DEMO_CODEC_AUTH set, it refuses requests whose Authorization header isn't exactly that
// value; set TEMPORAL_CODEC_AUTH to the same on the connector programs.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/temporalio/nexus-connector-kafka/settings"
	"github.com/temporalio/nexus-connector-kafka/test/encryption"
	"go.temporal.io/sdk/converter"
)

// uiOrigin is the dev server's UI, which calls the codec server from the browser.
const uiOrigin = "http://localhost:8233"

func main() {
	addr := flag.String("addr", "localhost:8089", "the address to listen on")
	flag.Parse()

	codec, err := encryption.NewCodec(encryption.Key())
	if err != nil {
		log.Fatalf("Set %s to the demo's passphrase: %v", encryption.KeyEnvVar, err)
	}
	auth := settings.Get(settings.OS(), "DEMO_CODEC_AUTH")
	handler := converter.NewPayloadCodecHTTPHandler(codec)

	log.Printf("Serving the demo payload codec on http://%s", *addr)
	server := &http.Server{Addr: *addr, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The UI calls from the browser, so it needs CORS.
		w.Header().Set("Access-Control-Allow-Origin", uiOrigin)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,X-Namespace,Authorization")
		if r.Method == http.MethodOptions {
			return
		}
		if auth != "" && r.Header.Get("Authorization") != auth {
			http.Error(w, "unauthorized: the Authorization header doesn't match DEMO_CODEC_AUTH", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	})}
	log.Fatal(server.ListenAndServe())
}
