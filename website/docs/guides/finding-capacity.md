---
id: finding-capacity
title: Finding capacity
sidebar_label: Finding capacity
description: Step the rate up past a server's capacity and read where latency turns.
---

The capacity of a service is the highest rate it sustains with acceptable
latency. Because diapason keeps its schedule whatever the server does, going
past capacity shows up as growing latency and in-flight requests, not as a
quietly lower rate.

## A server of known capacity

```bash
diapason server --workers 4 --service exp:2ms
```

4 virtual servers with a mean service time of 2 ms: the capacity is
4 / 2 ms = 2000 requests per second.

## A step profile

Save this as `steps.toml`:

```toml title="steps.toml"
seed = 1
threads = 2

[target]
url = "http://127.0.0.1:8080/"

[load]
arrival = "poisson"

[[load.phases]]
name = "warmup"
duration = "2s"
rate = 500

[[load.phases]]
name = "measure"
steps = [
  { duration = "4s", rate = 1000 },
  { duration = "4s", rate = 1600 },
  { duration = "4s", rate = 1900 },
  { duration = "4s", rate = 2100 },
]

[connections]
count = 50
max = 5000

[output]
window = "2s"
windows = "steps.jsonl"
```

```bash
diapason run --config steps.toml --summary steps.json
```

`max = 5000` allows connect-on-demand, so that above capacity the queue
forms in the server, as it would with many real clients, and not in the
client.

## Read it window by window

The window file has one JSON line per 2-second window:

```bash
jq -r '[.start_s, .scheduled, .completed, .in_flight_mean, .corrected_p50_ms, .corrected_p99_ms] | @tsv' steps.jsonl
```

| window start (s) | scheduled | completed | in flight | p50 (ms) | p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| 0 | 964 | 964 | 1.1 | 1.7 | 9.2 |
| 2 | 2005 | 2002 | 2.4 | 1.9 | 9.4 |
| 4 | 2027 | 2030 | 2.5 | 1.9 | 10.0 |
| 6 | 3224 | 3210 | 6.2 | 3.0 | 13.0 |
| 8 | 3133 | 3145 | 6.2 | 3.2 | 12.9 |
| 10 | 3846 | 3840 | 15.2 | 6.9 | 23.2 |
| 12 | 3895 | 3857 | 18.9 | 7.2 | 32.1 |
| 14 | 4249 | 4035 | 141.8 | 68.0 | 119.3 |
| 16 | 4248 | 3941 | 442.1 | 215.4 | 272.6 |
| 18 | 0 | 567 | 37.7 | 277.3 | 289.4 |

- Up to 1900 requests per second (95 % of capacity), everything that is
  scheduled completes, and the p99 grows from 9 to 32 ms as the queue at the
  server lengthens.
- At 2100 requests per second the server completes about 2000 per second
  (4035 in a 2-second window) whatever is offered, requests in flight pile
  up (from 142 to 442), and the median latency climbs by 150 ms in 2
  seconds. The system is past capacity.
- The last window is the drain: no new requests, the backlog completes.

Windows are counted by the time of each event (scheduling, completion), so
they show what happened when. The phase blocks of the summary count by
intended time instead; see [Phases](../accounting-model#phases).

## Ramps and other profiles

A linear ramp in one phase:

```toml
[[load.phases]]
name = "measure"
duration = "6s"
rate = 200
rate_end = 3000
```

This schedules 9600 requests (the area under the ramp, a mean of 1600 per
second). `points = [{ at = "0s", rate = 100 }, { at = "30s", rate = 900 }]`
gives a piecewise-linear profile, and `arrival = "replay"` with
`replay_file = "times.txt"` sends requests at times read from a file, one
number of seconds per line:

```bash
python3 -c "
import random; random.seed(1); t = 0.0
for _ in range(3000):
    t += random.expovariate(1000); print(f'{t:.6f}')" > times.txt
diapason run --url http://127.0.0.1:8080/ --arrival replay --replay-file times.txt --duration 4s
```

## Search for the capacity automatically

The `rate_search` example drives diapason through its library: it bisects
the rate, runs each step on a thread, watches the windows, and stops a step
early when three windows in a row exceed the latency target. A step counts
only if the client's fidelity checks passed.

```bash
cargo run --release -p diapason-cli --example rate_search
```

```text
target: p99 of corrected latency <= 10 ms on a reference server (capacity 8000 req/s)
    1414 req/s: ok   (p99 5.07 ms, 1406 req/s achieved)
    5318 req/s: ok   (p99 4.87 ms, 5358 req/s achieved)
   10313 req/s: fail (stopped at 3 s: p99 684.72 ms in 3 windows)
    7406 req/s: ok   (p99 8.47 ms, 7482 req/s achieved)
    8739 req/s: fail (stopped at 3 s: p99 283.38 ms in 3 windows)
    8045 req/s: fail (stopped at 3 s: p99 47.68 ms in 3 windows)
    7719 req/s: fail (stopped at 8 s: p99 10.24 ms in 3 windows)
highest rate meeting the target: about 7406 req/s
```

Give it a URL and a target to test another service:
`cargo run --release -p diapason-cli --example rate_search -- http://host:port/ 50`.
