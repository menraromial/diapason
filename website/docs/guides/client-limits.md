---
id: client-limits
title: When the client is the bottleneck
sidebar_label: Client limits
description: Too few connections, pipelining, connect-on-demand, file-descriptor limits and CPU starvation, and how diapason reports each.
---

A load test measures the server only if the client delivered the load. Most
tools stay silent when they did not. diapason checks it and exits with code
3 when the client shaped the load. This guide provokes each case against a
fast server:

```bash
diapason server --capacity 20000
```

## Too few connections

With HTTP/1.1, a connection carries one request at a time. Two connections
and a 1 ms service time cap the client near 2 × 1/1.2 ms ≈ 1700 requests per
second, far from the 5000 asked:

```bash
diapason run --url http://127.0.0.1:8080/ --rate 5000 --duration 5s \
  --threads 1 --connections 2
```

```text
  connection wait    5045.748  9227.469 10074.718 10166.993 10175.382  5067.158
fidelity: VIOLATED (ADMISSION-BOUND: the client's connection or in-flight limits shaped the load)
  FAIL send_rate                  0.3290 limit     0.9900  requests sent in the interval / requests scheduled in it
  FAIL admission                  0.9662 limit     0.0100  share of requests that waited more than 1ms for a connection or slot, or were dropped from the queue
exit 3: fidelity checks failed: send_rate, send_rate_worker_0, admission
```

Only a third of the scheduled requests were sent during the measurement,
and the others waited up to 10 seconds in the client's queue. Corrected
latency includes that wait, so the percentiles are not too good to be true;
but the server never saw 5000 requests per second, and the run says so.

Three ways out:

**More connections**, the plain answer: `--connections 100`.

**Pipelining**, several requests in flight per HTTP/1.1 connection:

```bash
diapason run --url http://127.0.0.1:8080/ --rate 5000 --duration 5s \
  --threads 1 --connections 2 --pipeline 16
```

```text
  corrected             1.195     1.610     2.167     2.681    41.386     1.284
  connection wait       0.002     0.003     0.011     0.045     0.258     0.002
exit 0: ok
```

**Connect-on-demand**, new connections while requests wait, up to a cap:

```bash
diapason run --url http://127.0.0.1:8080/ --rate 5000 --duration 5s \
  --threads 1 --connections 2 --max-connections 200 --summary od.json
jq '.connections | {opened_on_demand, peak_open}' od.json
```

```json
{
  "opened_on_demand": 15,
  "peak_open": 17
}
```

The cap matters: a client that opens connections without limit pushes an
unbounded concurrency into a server in trouble. diapason reports how many it
opened, and the window file gives `connections_open_max` per window.

With HTTP/2, one connection carries many streams: `--http2` and
`http.h2_max_streams` (default 100) replace `--pipeline`. See
[HTTP/2 and TLS](http2-and-tls).

## File descriptors

Each connection needs a file descriptor. diapason raises its soft limit to
the hard limit at start-up and refuses to start when even the hard limit is
too low:

```bash
prlimit --nofile=256:256 diapason run --url http://127.0.0.1:8080/ \
  --rate 100 --duration 2s --connections 1000 --threads 2
```

```text
diapason: configuration error: file-descriptor limit too low: RLIMIT_NOFILE is 256 (hard 256), the run needs 1096 (1000 connections at most + fd_margin 64 + 16 per thread for 2 threads)
```

The exit code is 2. A tool that does not check this opens the connections it
can, and the missing ones turn into errors or timeouts that look like the
server's fault.

## A client starved of CPU

Pinning the client to one CPU while asking for 100 000 requests per second:

```bash
taskset -c 0 diapason run --url http://127.0.0.1:8080/ --rate 100000 \
  --duration 3s --threads 4 --connections 200
```

```text
  FAIL admission                  0.8636 limit     0.0100  share of requests that waited more than 1ms for a connection or slot, or were dropped from the queue
  FAIL worker_runqueue_0          0.1753 limit     0.0500  share of wall time the worker was runnable but not running
  FAIL worker_runqueue_1          0.1802 limit     0.0500  share of wall time the worker was runnable but not running
  ...
exit 3: fidelity checks failed: generator_lag_p99, send_rate, ..., admission, worker_runqueue_0, ...
```

On Linux, `worker_runqueue_N` reads how long each worker thread waited for
a CPU; here 18 % of the time. The generator lag and send-rate checks fail
too: the client could not keep its schedule.

## What the checks look at

| symptom | check | typical cause |
| --- | --- | --- |
| requests leave late | `generator_lag_p99`, `generator_lag_max` | CPU starvation, too few threads |
| fewer requests sent than scheduled | `send_rate`, `send_rate_worker_N` | not enough connections or CPU |
| requests leave in bursts | `burstiness_10ms`, `burstiness_50ms` | stalls of the client |
| requests wait for a connection | `admission` | pool or in-flight bound too small |
| connections missing at the start | `connections` | file descriptors, ports, server refusing |
| a worker thread cannot keep up | `worker_busy_N`, `worker_runqueue_N` | too much work per thread, CPU shared |

Thresholds are in the `[fidelity]` table; for example
`--set fidelity.lag_p99=2ms`. See [Fidelity checks](../fidelity-checks).
