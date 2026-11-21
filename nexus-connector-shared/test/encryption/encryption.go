// Package encryption is a demo payload codec: AES-256-GCM with a key derived from a passphrase.
// For testing and the demo only. A real codec gets its key from a key management service, rotates
// keys, and keeps the key ID it encrypted with, as this one does, so it can still decrypt
// payloads written with an older key.
//
// The connector programs use it through the demo codec server (codecserver), with
// TEMPORAL_CODEC_ENDPOINT. The repo's own demo workers, where it has them (a test caller, a
// receiver), use it directly, in their blocks marked ENCRYPTION. All of them must use the same key.
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/settings"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// KeyEnvVar is the variable the demo programs read the passphrase from. Unset, they don't encrypt.
const KeyEnvVar = "DEMO_ENCRYPTION_KEY"

const (
	encodingEncrypted = "binary/encrypted"
	metadataKeyID     = "encryption-key-id"
)

// Key is the passphrase in KeyEnvVar, or "" if it isn't set.
func Key() string { return settings.Get(settings.OS(), KeyEnvVar) }

// Codec encrypts every payload with AES-256-GCM under a key derived from passphrase.
type Codec struct {
	aead  cipher.AEAD
	keyID string
}

var _ converter.PayloadCodec = (*Codec)(nil)

// NewCodec returns a Codec for passphrase, which must not be empty.
func NewCodec(passphrase string) (*Codec, error) {
	if passphrase == "" {
		return nil, fmt.Errorf("%s is empty", KeyEnvVar)
	}
	key := sha256.Sum256([]byte(passphrase))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	id := sha256.Sum256(key[:])
	return &Codec{aead: aead, keyID: hex.EncodeToString(id[:4])}, nil
}

// DataConverter is the default data converter with Codec, for a demo worker's client options.
func DataConverter(passphrase string) (converter.DataConverter, error) {
	c, err := NewCodec(passphrase)
	if err != nil {
		return nil, err
	}
	return converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), c), nil
}

// FailureConverter encodes errors' details, messages and stack traces with dc, as the connector
// programs do when a codec is set, so a demo worker can read the errors they return and encrypts
// its own.
func FailureConverter(dc converter.DataConverter) converter.FailureConverter {
	return temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: dc, EncodeCommonAttributes: true})
}

// Encode encrypts each payload, whole (its metadata and data), into a new payload's data.
func (c *Codec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plain, err := p.Marshal()
		if err != nil {
			return nil, err
		}
		nonce := make([]byte, c.aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		out[i] = &commonpb.Payload{
			Metadata: map[string][]byte{
				converter.MetadataEncoding: []byte(encodingEncrypted),
				metadataKeyID:              []byte(c.keyID),
			},
			Data: c.aead.Seal(nonce, nonce, plain, nil),
		}
	}
	return out, nil
}

// Decode decrypts payloads Encode encrypted, and passes any other payload through unchanged.
func (c *Codec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		if string(p.GetMetadata()[converter.MetadataEncoding]) != encodingEncrypted {
			out[i] = p
			continue
		}
		if id := string(p.GetMetadata()[metadataKeyID]); id != c.keyID {
			return nil, fmt.Errorf("payload encrypted with key %q, but this codec has key %q: set the same %s everywhere", id, c.keyID, KeyEnvVar)
		}
		data := p.GetData()
		if len(data) < c.aead.NonceSize() {
			return nil, errors.New("encrypted payload too short")
		}
		plain, err := c.aead.Open(nil, data[:c.aead.NonceSize()], data[c.aead.NonceSize():], nil)
		if err != nil {
			return nil, fmt.Errorf("decrypt payload: %w", err)
		}
		var decoded commonpb.Payload
		if err := decoded.Unmarshal(plain); err != nil {
			return nil, err
		}
		out[i] = &decoded
	}
	return out, nil
}
