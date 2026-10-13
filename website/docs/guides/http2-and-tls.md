---
id: http2-and-tls
title: HTTP/2 and TLS
sidebar_label: HTTP/2 and TLS
description: Test HTTPS with a self-signed certificate, HTTP/2 over TLS and cleartext, and stream limits.
---

## A certificate for local tests

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
  -keyout key.pem -out cert.pem -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE"
```

`CA:FALSE` matters: rustls refuses a CA certificate used as a server
certificate.

## HTTPS

```bash
diapason server --listen 127.0.0.1:8443 --workers 8 --service exp:1ms \
  --tls-cert cert.pem --tls-key key.pem
```

```bash
diapason run --url https://localhost:8443/ --ca-file cert.pem \
  --rate 2000 --duration 4s --threads 2 --connections 8
```

```text
diapason 0.1.0 (c0521ef00ccd) open mode, seed 1, 2 threads, https/1.1 127.0.0.1:8443
connections: 8/8 established, shortfall none, connect errors none, reconnects 0, closes server/client/error 0/0/0
  corrected             0.935     2.574     4.977     7.496    12.272     1.249
  server                0.909     2.540     4.973     7.492    12.263     1.227
exit 0: ok
```

The TLS handshake happens while connections open, before the run starts,
so it is not part of any request's latency. Without `--ca-file`, the
certificate is not trusted and every connection fails in the handshake:

```text
connections: 0/2 established, shortfall tls=2, connect errors tls=36, reconnects 0, closes server/client/error 0/0/0
  FAIL connections                0.0000 limit     2.0000  connections established before T0 / requested
```

Other options: `--client-cert` and `--client-key` for client certificates,
`--set tls.server_name=api.example.org` to send another name in SNI,
`--insecure` to accept any certificate (for tests only).

## HTTP/2

`--http2` sends each request on its own stream. Over TLS, HTTP/2 is
negotiated with ALPN:

```bash
diapason run --url https://localhost:8443/ --http2 --ca-file cert.pem \
  --rate 2000 --duration 4s --threads 2 --connections 8
```

```text
diapason 0.1.0 (c0521ef00ccd) open mode, seed 1, 2 threads, h2 127.0.0.1:8443
  corrected             0.968     2.576     4.940     6.803     8.368     1.262
exit 0: ok
```

Over cleartext, diapason uses prior knowledge (h2c), and one connection is
enough for thousands of requests per second:

```bash
diapason server --workers 8 --service exp:1ms &
diapason run --url http://127.0.0.1:8080/ --http2 --rate 5000 --duration 4s \
  --threads 1 --connections 1
```

```text
  achieved 5013.8/s by intended time, 5013.0/s by completion time; failed: none; not sent: none; in flight at end: 0
  corrected             0.912     2.519     4.760     6.975    10.265     1.204
  in flight: mean 6.0 (Little's law predicts 6.0, +0.0 %), in system: mean 6.0
exit 0: ok
```

## Stream limits

`http.h2_max_streams` (default 100) bounds the streams per connection. Set
it too low and the run is admission-bound, like an HTTP/1.1 run with too few
connections:

```bash
diapason run --url http://127.0.0.1:8080/ --http2 --set http.h2_max_streams=4 \
  --rate 5000 --duration 4s --threads 1 --connections 1
```

```text
  connection wait     923.271  1682.964  1841.299  1852.834  1854.931   927.761
fidelity: VIOLATED (ADMISSION-BOUND: the client's connection or in-flight limits shaped the load)
exit 3: fidelity checks failed: send_rate, send_rate_worker_0, admission
```

With HTTP/2 a request timeout resets its own stream and leaves the other
streams of the connection alone; the summary counts these in
`connections.cancelled`.
