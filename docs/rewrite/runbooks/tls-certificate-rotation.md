# Runbook: TLS Certificate Rotation

## Overview

This runbook covers replacing TLS certificates on Cassandra Rust nodes
without service interruption.

## Prerequisites

- New PEM certificate and key files ready
- CA certificate if using mutual TLS
- Access to the node filesystem

## Procedure

### 1. Validate new certificates

```bash
# Check certificate validity
openssl x509 -in /path/to/new/cert.pem -text -noout

# Verify key matches certificate
openssl x509 -noout -modulus -in cert.pem | md5sum
openssl rsa  -noout -modulus -in key.pem  | md5sum
# Both should match
```

### 2. Stage new certificates

```bash
# Copy to staging directory first
cp new-cert.pem /etc/cassandra/certs/cert.pem.new
cp new-key.pem  /etc/cassandra/certs/key.pem.new
```

### 3. Replace certificates

```bash
# Atomic replace
mv /etc/cassandra/certs/cert.pem.new /etc/cassandra/certs/cert.pem
mv /etc/cassandra/certs/key.pem.new  /etc/cassandra/certs/key.pem
```

### 4. Verify reload

If `hot_reload` is enabled in `cassandra.yaml`, the server will automatically
pick up the new certificates within the configured `reload_interval_secs`.

Check the server logs for:
```
INFO TLS certificates reloaded successfully
```

### 5. Verify connections

```bash
# Test TLS connection
openssl s_client -connect localhost:9042 -servername localhost < /dev/null

# Verify with cqlsh (if available)
cqlsh --ssl localhost 9042
```

## Rollback

If the new certificate causes issues:
1. Restore the old certificate files
2. The hot-reload mechanism will pick up the old certs
3. No restart required

## Converting from JKS (Java keystore)

```bash
# Export certificate from JKS
keytool -exportcert -keystore keystore.jks -alias node0 -rfc > node.crt

# Export private key from JKS (via PKCS12)
keytool -importkeystore -srckeystore keystore.jks \
  -destkeystore keystore.p12 -deststoretype PKCS12
openssl pkcs12 -in keystore.p12 -nocerts -nodes -out node.key
openssl pkcs12 -in keystore.p12 -nokeys -out node.crt

# Export CA from truststore
keytool -exportcert -keystore truststore.jks -alias caroot -rfc > ca.crt
```

## Configuration reference

```yaml
# cassandra.yaml
client_encryption_options:
  enabled: true
  certificate: /etc/cassandra/certs/node.crt
  certificate_key: /etc/cassandra/certs/node.key
  ca_certificate: /etc/cassandra/certs/ca.crt
  require_client_auth: false

server_encryption_options:
  enabled: true
  certificate: /etc/cassandra/certs/node.crt
  certificate_key: /etc/cassandra/certs/node.key
  ca_certificate: /etc/cassandra/certs/ca.crt
  require_client_auth: true
```
