---
id: coordinated-omission
title: Open loop and coordinated omission
sidebar_label: Coordinated omission
description: Freeze the server for one second and compare what a closed-loop and an open-loop client report.
---

A closed-loop client sends a request, waits for the response, then sends the
next one. When the server stalls, the client stalls with it: the requests it
would have sent during the stall are never sent, so they never show up in
the percentiles. This is **coordinated omission**, and it makes a frozen
server look healthy.

An open-loop client sends each request at its scheduled time whatever
happens to the previous ones. This guide reproduces the difference on your
machine.

## The server

The reference server can slow down for a while: `--slowdown 6s:1s:1000`
multiplies service times by 1000 for requests whose service starts between
6 and 7 seconds after the server starts. With an exponential service time of
mean 1 ms, the server freezes for about a second.

```bash
diapason server --workers 8 --service exp:1ms --slowdown 6s:1s:1000
```

The slowdown is timed from the server's start, so restart the server before
each run below.

## A closed-loop run

`--closed-loop` makes each connection send its next request as soon as the
previous one finishes. With 2 connections:

```bash
diapason run --url http://127.0.0.1:8080/ --closed-loop \
  --warmup 1s --duration 10s --threads 1 --connections 2 --seed 1
```

```text
CLOSED-LOOP run: no schedule; latencies suffer from coordinated omission.
...
phase measure (measured) [1.0s, 11.0s): scheduled 12543 (1254.3/s, nominal 0.0/s), completed 12543, success 12543
  ms                      p50       p90       p99     p99.9       max      mean
  corrected             0.970     2.583     4.956     7.545  2134.901     1.593
  server                0.967     2.580     4.956     7.541  2134.901     1.590
```

diapason labels the mode on the first line of the report and in the summary
(`"mode": "closed"`). The client managed 1254 requests per second, and the
freeze appears only in the maximum: 2 requests were stuck in it, out of
12 543. The p99 says 5 ms.

## An open-loop run at the same rate

Now schedule the same 1254 requests per second, whatever the server does:

```bash
diapason run --url http://127.0.0.1:8080/ --rate 1254 \
  --warmup 1s --duration 10s --threads 1 \
  --connections 2 --max-connections 4000 --seed 1 \
  --summary summary.json
```

```text
phase measure (measured) [1.0s, 11.0s): scheduled 12669 (1266.9/s, nominal 1254.0/s), completed 12669, success 12669
  ms                      p50       p90       p99     p99.9       max      mean
  corrected             1.210   621.806  1150.288  1176.502  5242.880   122.445
  server                1.205   621.806  1149.239  1176.502  5242.880   122.370
  connection wait       0.005     0.050     0.160     1.412    17.793     0.025
  generator lag         0.000     0.001     0.098    13.328    17.531     0.050
...
fidelity: ok
```

| | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- |
| closed loop, 2 connections | 0.97 ms | 2.6 ms | 5.0 ms | 2.1 s |
| open loop, same rate | 1.21 ms | 622 ms | 1150 ms | 5.2 s |

About 1250 requests were scheduled during the freeze, and every one of them
waited. That is what the users of this server would have experienced.

`--max-connections 4000` lets diapason open connections while requests wait
for one (connect-on-demand). Without it, the 2 connections would have
filled up and the requests would have waited in the client's queue instead:
corrected latency would still include the wait, but the server would not
have seen the load, and the run would fail its `admission` check. Here the
summary shows how many connections the freeze forced open:

```bash
jq '.connections | {opened_on_demand, peak_open}' summary.json
```

```json
{
  "opened_on_demand": 1524,
  "peak_open": 1526
}
```

## What to take away

- Compare tools at the same rate, not the same connection count.
- A closed-loop number answers "how fast can 2 clients go"; an open-loop
  number answers "what latency do users see at this rate".
- diapason's closed-loop mode exists for such comparisons, is never the
  default, and says so in every output.
