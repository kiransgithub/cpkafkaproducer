#!/bin/bash

# Set variables
COUNTRY="US"
STATE="Arizona"
LOCALITY="Phoenix"
ORGANIZATION="MyTestOrg"
ORGANIZATIONAL_UNIT="IT"
COMMON_NAME="localhost"
DAYS=365

# Generate CA key and certificate
openssl req -x509 -newkey rsa:4096 -days $DAYS -nodes -keyout ca-key.pem -out ca-cert.pem -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORGANIZATIONAL_UNIT/CN=$COMMON_NAME CA"

# Generate server key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -keyout server.key -out server.csr -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORGANIZATIONAL_UNIT/CN=$COMMON_NAME"

# Sign the server certificate with CA
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server.crt -days $DAYS

# Generate client key and certificate signing request (CSR)
openssl req -newkey rsa:4096 -nodes -keyout client.key -out client.csr -subj "/C=$COUNTRY/ST=$STATE/L=$LOCALITY/O=$ORGANIZATION/OU=$ORGANIZATIONAL_UNIT/CN=$COMMON_NAME Client"

# Sign the client certificate with CA
openssl x509 -req -in client.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client.crt -days $DAYS

# Clean up intermediate files
rm server.csr client.csr ca-cert.srl

echo "Certificate generation complete."
