---
id: first-run
title: Your first run
sidebar_label: Your first run
description: Start the reference server, send load at a fixed rate, and read every line of the report.
---

This guide takes five minutes on any Linux or macOS machine. It uses the
reference server that ships with diapason, so nothing else needs to run.

## 1. Start a server

```bash title="terminal 1"
diapason server --capacity 20000
```

```text
listening 127.0.0.1:8080 (workers 20, service const:1ms, capacity 20000.0 req/s, threads 11)
```

The reference server models a queue with 20 virtual servers that each take
1 ms per request, so it can sustain 20 000 requests per second. Its capacity
does not depend on the CPU it gets, which makes it a predictable target. The
[reference server guide](reference-server) lists its options.

## 2. Send load

```bash title="terminal 2"
diapason run --url http://127.0.0.1:8080/ --rate 5000 \
  --warmup 2s --duration 10s --threads 4 --connections 100 \
  --summary out.json
```

This schedules requests at 5000 per second (Poisson arrivals by default) for
a 2-second warmup phase and a 10-second measurement phase, from 4 worker
threads over 100 keep-alive connections, and writes the JSON summary to
`out.json`.

## 3. Read the report

```text
diapason 0.1.0 (c0521ef00ccd) open mode, seed 1, 4 threads, http/1.1 127.0.0.1:8080
started 2026-10-13T20:07:15.995880Z, wall 12.0s
requests: scheduled 60350 = completed 60350 + failed 0 + not sent 0 + in flight 0 -> balanced
connections: 100/100 established, shortfall none, connect errors none, reconnects 0, closes server/client/error 0/0/0
```

The first lines identify the run (version, mode, seed, protocol) and give the
**conservation check**: every scheduled request ended as completed, failed,
not sent, or still in flight. If the books do not balance, diapason exits
with code 4. The seed is drawn at random when you do not give one; pass
`--seed 1` to replay the same schedule.

```text
phase measure (measured) [2.0s, 12.0s): scheduled 50317 (5031.7/s, nominal 5000.0/s), completed 50317, success 50317
  achieved 5031.7/s by intended time, 5031.3/s by completion time; failed: none; not sent: none; in flight at end: 0
  status: 200=50317
  ms                      p50       p90       p99     p99.9       max      mean
  corrected             1.187     1.335     1.460     1.561     2.265     1.196
  server                1.181     1.325     1.444     1.532     2.179     1.189
  connection wait       0.003     0.007     0.018     0.056     0.331     0.004
  generator lag         0.000     0.002     0.062     0.130     0.308     0.003
  in flight: mean 6.0 (Little's law predicts 6.0, +0.0 %), in system: mean 6.0
```

Each phase gets its own block. A request belongs to the phase of its
**intended** time, the time the schedule gave it.

- `scheduled 50317 (5031.7/s, nominal 5000.0/s)`: Poisson arrivals are
  random, so the offered rate differs slightly from the nominal one.
- `achieved ... by intended time` counts completed requests by the phase they
  were scheduled in; `by completion time` counts what finished during the
  phase. Above capacity the second one is the server's throughput.
- The four latency rows are the four delays of the
  [accounting model](../accounting-model): **corrected** latency is what a user
  would have seen; it is the sum of **generator lag** (the client was late),
  **connection wait** (no free connection) and **server** time. Here the
  client added about 6 µs at p99.
- The in-flight line checks Little's law: the mean number of requests in
  flight equals the completion rate times the mean time in flight.

```text
fidelity: ok
  ok   generator_lag_p99          0.0616 limit     1.0000  p99 of attempt - intended, in ms
  ok   generator_lag_max          0.3080 limit    50.0000  max of attempt - intended, in ms
  ok   send_rate                  1.0000 limit     0.9900  requests sent in the interval / requests scheduled in it
  ok   burstiness_10ms            1.3912 limit     2.7824  sends peak/mean 1.391 (schedule 1.391), empty slices 0.0000 (schedule 0.0000, slack 0.01)
  ok   burstiness_50ms            1.1726 limit     1.7588  sends peak/mean 1.173 (schedule 1.173), empty slices 0.0000 (schedule 0.0000, slack 0.01)
  ok   connections              100.0000 limit   100.0000  connections established before T0 / requested
  ok   admission                  0.0000 limit     0.0100  share of requests that waited more than 1ms for a connection or slot, or were dropped from the queue
  busiest worker: 3.3 % busy
exit 0: ok
```

The **fidelity checks** answer one question: did the server receive the load
you asked for? The client was on time (lag), sent as many requests as
scheduled (send rate), did not bunch them up (burstiness), opened all its
connections, never ran short of connections (admission), and had CPU to
spare. When one fails, the run exits with code 3 and the line says
`FAIL`. The [fidelity checks](../fidelity-checks) page explains each one.

## 4. Look at the JSON summary

Everything in the report, and more, is in `out.json`:

```bash
jq '.phases[] | select(.measured) | {offered_rate, achieved_rate_by_completion, p99: .latency.corrected.p99_ms}' out.json
```

```json
{
  "offered_rate": 5031.7,
  "achieved_rate_by_completion": 5031.3,
  "p99": 1.460223
}
```

The [outputs reference](../outputs) describes every field, and
[Analysing results](analysing-results) shows how to work with the summary,
the window file and the per-request trace.

## Exit codes

| code | meaning |
| --- | --- |
| 0 | the run completed, the books balanced, the fidelity checks passed |
| 1 | runtime error |
| 2 | configuration error (including a file-descriptor limit too low) |
| 3 | a fidelity check failed: the client did not deliver the configured load |
| 4 | conservation broken: a bug in diapason |
| 5 | a user threshold failed (see [CI performance gates](ci-gates)) |

## Next steps

- [Open loop and coordinated omission](coordinated-omission): why a
  closed-loop tool can report a p99 of 5 ms for a server that froze for a
  second.
- [Finding capacity](finding-capacity): step profiles and reading the load
  window by window.
- [Cookbook](cookbook): one-line commands for common tasks.
