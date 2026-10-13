---
id: distributed
title: Several client machines
sidebar_label: Distributed runs
description: Run one load test from several agents, with clock-offset measurement and merged results.
---

One client machine runs out of CPU, network or ports at some point. In
distributed mode a coordinator splits one run across agents on several
machines and merges their results into one summary.

## Try it on one machine

Two agents, in two terminals:

```bash
diapason agent --listen 127.0.0.1:7700
diapason agent --listen 127.0.0.1:7701
```

A server and the coordinator:

```bash
diapason server --workers 8 --service exp:1ms &
diapason run --agents 127.0.0.1:7700,127.0.0.1:7701 \
  --url http://127.0.0.1:8080/ --rate 4000 --warmup 1s --duration 5s \
  --threads 2 --connections 40 --seed 3 --summary dist.json
```

```text
diapason 0.1.0 (c0521ef00ccd) open mode, seed 3, 4 threads, http/1.1 127.0.0.1:8080
started 2026-10-13T20:14:22.615730Z, wall 6.0s
requests: scheduled 23729 = completed 23729 + failed 0 + not sent 0 + in flight 0 -> balanced
connections: 40/40 established, shortfall none, connect errors none, reconnects 0, closes server/client/error 0/0/0
phase measure (measured) [1.0s, 6.0s): scheduled 19758 (3951.6/s, nominal 4000.0/s), completed 19758, success 19758
  corrected             0.919     2.525     4.723     6.865     9.708     1.206
```

`--threads 2` applies to each agent: the run has 4 workers, 2 per agent.
The coordinator generates no load; it writes the summary and serves the live
metrics.

```bash
jq '.distributed.agents[] | {address, first_worker, threads, clock_offset_ms, rtt_ms}' dist.json
```

```json
{
  "address": "127.0.0.1:7700",
  "first_worker": 0,
  "threads": 2,
  "clock_offset_ms": -0.000132,
  "rtt_ms": 0.035636
}
{
  "address": "127.0.0.1:7701",
  "first_worker": 2,
  "threads": 2,
  "clock_offset_ms": -0.000616,
  "rtt_ms": 0.03489
}
```

## On real machines

Start an agent on each client machine (it listens on `0.0.0.0:7700` by
default) and give their addresses to the coordinator. The protocol between
them is neither authenticated nor encrypted: use a trusted network.

## What you get

- **The same requests as one machine.** Agent k runs a range of the global
  worker numbers, and each worker takes its share of the schedule from its
  number. With the same seed, a 2 × 2 run sends exactly the requests of a
  one-machine run with 4 threads; the validation tests compare the two
  traces.
- **One measurement.** Agents forward their workers' raw histograms and
  counts; the coordinator merges them, so quantiles and fidelity checks
  cover all agents together.
- **Aligned start.** The coordinator measures each agent's clock offset over
  32 exchanges, keeps the one with the shortest round trip (the offset is
  known to within half of it), and sends each agent the start time in its
  own clock. In the validation test, an agent with its clock moved 500 ms
  ahead was measured at 500.001 ms and started within 0.1 µs of the other.
- **Failures are reported.** If an agent dies, the coordinator stops with
  exit code 1 and names it.

Traces stay on the agents, one file per agent with its worker range in the
name (`w0-1.out.dtr`).
