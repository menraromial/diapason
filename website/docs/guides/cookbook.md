---
id: cookbook
title: Cookbook
sidebar_label: Cookbook
description: Short commands for common tasks.
---

Every command assumes a reference server on `127.0.0.1:8080`
(`diapason server --capacity 20000`). Replace the URL with your service's.

## Rates and durations

```bash
# 1000 req/s for 30 s after a 5 s warmup (Poisson arrivals)
diapason run --url http://127.0.0.1:8080/ --rate 1000 --warmup 5s --duration 30s

# evenly spaced arrivals instead of Poisson
diapason run --url http://127.0.0.1:8080/ --rate 1000 --arrival constant --duration 30s

# warmup, measurement and cooldown phases at the same rate
diapason run --url http://127.0.0.1:8080/ --rate 1000 --warmup 10s --duration 60s --cooldown 10s

# the same schedule as yesterday's run
diapason run --url http://127.0.0.1:8080/ --rate 1000 --duration 30s --seed 42
```

Ramps, steps and piecewise profiles need a configuration file; see
[Finding capacity](finding-capacity).

## Requests

```bash
# POST with headers and a body
diapason run --url http://127.0.0.1:8080/orders --method POST \
  -H "content-type: application/json" --body '{"item": 1}' --rate 200 --duration 10s

# HTTP/2 over cleartext (h2c) on 4 connections
diapason run --url http://127.0.0.1:8080/ --http2 --connections 4 --rate 5000 --duration 10s

# HTTPS with a private CA
diapason run --url https://localhost:8443/ --ca-file ca.pem --rate 500 --duration 10s

# only 2xx counts as success
diapason run --url http://127.0.0.1:8080/ --rate 500 --duration 10s --set 'http.success_status=["2xx"]'
```

## Connections and timeouts

```bash
# 500 keep-alive connections over 8 threads
diapason run --url http://127.0.0.1:8080/ --rate 20000 --threads 8 --connections 500 --duration 30s

# start with 50 connections, open more while requests wait, up to 5000
diapason run --url http://127.0.0.1:8080/ --rate 20000 --connections 50 --max-connections 5000 --duration 30s

# drop requests that wait more than 100 ms for a connection
diapason run --url http://127.0.0.1:8080/ --rate 5000 --max-queue-delay 100ms --duration 30s

# 2 s per-request timeout, 5 s drain at the end
diapason run --url http://127.0.0.1:8080/ --rate 500 --timeout 2s --drain 5s --duration 30s
```

## Outputs

```bash
# everything: summary, windows, histogram log, binary trace
diapason run --url http://127.0.0.1:8080/ --rate 1000 --duration 30s \
  --summary run.json --windows windows.jsonl --histogram-log run.hlog --trace run.dtr

# JSON summary on stdout, the text report on stderr
diapason run --url http://127.0.0.1:8080/ --rate 1000 --duration 10s --summary - | jq .conservation

# one line per second while it runs
diapason run --url http://127.0.0.1:8080/ --rate 1000 --duration 60s --progress

# convert a binary trace to compressed CSV
diapason trace run.dtr -o run.csv.zst
```

## Gates and checks

```bash
# fail (exit 5) when the p99 is above 50 ms or more than 0.1 % of requests fail
diapason run --url http://127.0.0.1:8080/ --rate 1000 --duration 30s \
  --threshold "p99 < 50ms" --threshold "error_rate < 0.1%"

# relax a fidelity threshold on a noisy machine
diapason run --url http://127.0.0.1:8080/ --rate 1000 --duration 30s --set fidelity.lag_p99=5ms

# check the installation
diapason selftest
```

## Several machines

```bash
diapason agent                                   # on each client machine
diapason run --agents host1:7700,host2:7700 --url http://target:8080/ --rate 50000 --duration 60s
```

## Comparing with a closed-loop tool

```bash
diapason run --url http://127.0.0.1:8080/ --closed-loop --connections 16 --duration 30s
```

The report starts with `CLOSED-LOOP` and its latencies suffer from
coordinated omission; see [Coordinated omission](coordinated-omission).
