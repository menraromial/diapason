---
id: reference-server
title: The reference server
sidebar_label: Reference server
description: A server of known capacity, with injectable slowdowns and faults, for trying diapason and testing a setup.
---

`diapason server` is a small HTTP server whose behaviour is known in
advance, so the numbers diapason reports can be checked against theory. The
validation tests and every guide on this site use it.

It models a queue with `c` servers in virtual time: each request is assigned
on arrival to the server that frees first, takes a service time drawn from
a distribution, and gets its response when its service ends. The capacity is
μ = c / E[service time], whatever CPU the process gets.

## Options

| option | meaning | default |
| --- | --- | --- |
| `--listen ADDR` | address to listen on | `127.0.0.1:8080` |
| `--workers c` | number of virtual servers; 0 answers at once | 0 |
| `--capacity μ` | sets `c = μ × mean service time` | |
| `--service D` | `const:1ms`, `exp:1ms`, `uniform:1ms:3ms`, `lognormal:1ms:0.5` (mean, sigma) | `const:1ms` |
| `--slowdown at:for:factor` | multiplies service times starting in `[at, at + for)` after the server's start; repeatable | |
| `--reset-prob p` | reset the connection | 0 |
| `--close-prob p` | answer with `Connection: close` and close | 0 |
| `--stall-prob p` | never answer | 0 |
| `--body-size n` | response body size in bytes | 2 |
| `--tls-cert`, `--tls-key` | serve TLS (HTTP/1.1 or HTTP/2 by ALPN) | |
| `--tls-client-ca` | require client certificates signed by this CA | |
| `--threads n` | runtime threads | half the cores |

Paths of the form `/status/NNN` answer with status NNN. HEAD requests get
headers only. A cleartext connection that starts with the HTTP/2 preface is
served as h2c.

## Examples

```bash
# 20 000 req/s, 1 ms per request
diapason server --capacity 20000

# 2000 req/s with exponential service times: a queue that behaves like M/M/4
diapason server --workers 4 --service exp:2ms

# answer at once, to measure the client's own overhead
diapason server --workers 0 --threads 8

# a 1-second freeze 6 seconds after start
diapason server --workers 8 --service exp:1ms --slowdown 6s:1s:1000

# 1 % resets, 1 % closes, 0.5 % unanswered requests
diapason server --workers 8 --reset-prob 0.01 --close-prob 0.01 --stall-prob 0.005
```

The server prints `listening ADDR (...)` when ready and, on Ctrl-C, its
counters as JSON on stderr:

```text
{"connections":100,"requests":60350,"responses":60350,"resets":0,"stalls":0,"closes":0,"bad_requests":0,"tls_errors":0}
```

On localhost, a server that answers at once gives a round trip of about
0.13 ms at p50; a constant 1 ms service gives about 1.16 ms.
