#!/bin/sh
# Makes a test CA, a broker certificate for localhost, and a client certificate for mTLS, in /certs
# (test/kafka/certs on the host), unless they exist. Run by the kafka-certs service. Test only:
# the keys are unencrypted and readable by anyone.
set -eu
cd /certs
if [ -f server-keystore.pem ]; then
  echo "Certificates exist in test/kafka/certs"
  exit 0
fi
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
  -subj "/CN=nexus-connector-kafka test CA" -keyout ca-key.pem -out ca.pem
issue() { # name subject extensions
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1-key.pem"
  openssl req -new -key "$1-key.pem" -subj "$2" -out "$1.csr"
  printf '%s\n' "$3" > "$1.ext"
  # 825 days at most: macOS refuses longer-lived server certificates as not standards compliant.
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days 825 \
    -extfile "$1.ext" -out "$1.pem"
  rm "$1.csr" "$1.ext"
}
issue server "/CN=localhost" "subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1
extendedKeyUsage=serverAuth"
issue client "/CN=connector" "extendedKeyUsage=clientAuth"
# Kafka's PEM keystore: the unencrypted PKCS#8 key, then the certificate.
cat server-key.pem server.pem > server-keystore.pem
chmod 644 ./*.pem
echo "Created certificates in test/kafka/certs"
