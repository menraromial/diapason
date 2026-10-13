---
id: failures
title: Failures, timeouts and conservation
sidebar_label: Failures and timeouts
description: Inject resets, closes and stalled requests, and see how every request is accounted for.
---

Errors are where load generators most often lose track of requests: a
connection that dies takes its requests with it, a timeout gets counted
twice, a request that was never sent disappears. diapason puts every
scheduled request in exactly one bucket and checks the sum at the end of
each run.

## Inject faults

The reference server can reset connections, close them after a response,
and leave requests unanswered:

```bash
diapason server --workers 8 --service exp:1ms \
  --reset-prob 0.01 --close-prob 0.01 --stall-prob 0.005
```

Each request has a 1 % chance of a connection reset, 1 % of a
`Connection: close` response, and 0.5 % of never getting an answer.

```bash
diapason run --url http://127.0.0.1:8080/ --rate 2000 --duration 5s \
  --threads 2 --connections 50 --timeout 500ms --seed 1 \
  --trace faults.csv --summary faults.json
```

```text
requests: scheduled 9975 = completed 9817 + failed 158 + not sent 0 + in flight 0 -> balanced
connections: 50/50 established, shortfall none, connect errors none, reconnects 252, closes server/client/error 94/48/110
  achieved 1963.4/s by intended time, 1963.2/s by completion time; failed: reset=110 timeout=48; not sent: none; in flight at end: 0
exit 0: ok
```

- 110 requests failed with `reset` and 48 with `timeout`: each timeout is
  counted once, for the request that waited 500 ms, after which diapason
  closed that connection (48 client closes).
- The server closed 94 connections after a response, and diapason
  reconnected 252 times in all.
- The books balance: 9817 + 158 = 9975.

The run exits with 0: the client delivered the load it was asked for, and
the failures are the server's. To make failures fail the run, add a
threshold such as `--threshold "error_rate < 0.5%"` (exit code 5); see
[CI performance gates](ci-gates).

## The buckets

```bash
jq .conservation faults.json
```

```json
{
  "scheduled": 9975,
  "completed": 9817,
  "failed": {
    "reset": 110,
    "timeout": 48
  },
  "failed_total": 158,
  "not_sent": {},
  "not_sent_total": 0,
  "in_flight_at_end": 0,
  "balance": 0,
  "ok": true
}
```

| bucket | meaning |
| --- | --- |
| `completed` | a full response came back, whatever its status code |
| `failed` | sent, but no full response: `timeout`, `aborted`, `reset`, `read`, `write`, `protocol` |
| `not_sent` | never written: waited too long in the client queue (`queue`), queue full (`queue_full`), or still queued at the end (`end_of_run`) |
| `in_flight_at_end` | sent, no outcome when the drain period ended |

Status codes are counted separately: a `500` is a completed request, and
`success` counts the codes in `http.success_status` (200 to 399 by
default).

## Every request in the trace

The trace has one row per scheduled request, with its outcome in the last
column (empty for completed requests):

```bash
cut -d, -f13 faults.csv | sort | uniq -c
```

```text
   9817
      1 error_kind
    110 reset
     48 timeout
```

## A server that dies

Kill the server in the middle of a run, and the requests that cannot find a
connection wait in the client's queue, then expire after
`connections.max_queue_delay` (60 s by default) or at the end of the drain:

```bash
diapason run --url http://127.0.0.1:8080/ --rate 500 --duration 5s \
  --max-queue-delay 1s --drain 2s
```

They end as `not_sent: queue`, and the connection errors (`connect_refused`)
are counted in the `connections` block, apart from the requests. The books
still balance.
