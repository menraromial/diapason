# diapason

diapason is an open-loop load generator for network services. It sends
requests on a schedule computed independently of the responses, accounts for
every scheduled request, records its own delays apart from the server's, and
states in its output whether it was itself the bottleneck. It does not judge
the system under test; it produces measurements that other tools analyse.

This README is the reference for the accounting model and for every output
field. The code follows it; a change to one without the other is a bug.

## Status

Milestones M1 and M2 are implemented. M1: constant, Poisson, ramp, step and
piecewise schedules, replay files, HTTP/1.1 with a bounded pool and optional
pipelining, per-request accounting, the JSON summary, fidelity checks, the
interval histogram log, the window file, the CSV trace, the reference server
and validation tests 1 to 9. M2: TLS (rustls) with SNI, client certificates
and an option to skip verification, request templates with variables and
data files, weighted scenarios with the realised mix, and user thresholds.
M3: HTTP/2 with a stream limit (h2 over TLS, h2c over cleartext),
connect-on-demand up to a cap, live OpenMetrics metrics, and the library API
(run on a thread, stop early, window channel). The trace is CSV (compressed
with zstd on request) or binary columnar. M4:
distributed mode, with a coordinator and agents on several client nodes,
clock-offset measurement, a split schedule and merged histograms.

Measured on one machine (22 logical CPUs, Linux 7.0) with the client and the
reference server (`--workers 0 --threads 10`) sharing it: 200 000 req/s of
HTTP/1.1 keep-alive with 10 client threads, 1000 connections and constant
arrivals passed every fidelity check in 5 runs out of 5 (2 s warmup, 8 s
measured). Generator lag was 1.4 to 2.9 µs at p99 and 1.2 to 3.0 ms at
most, corrected latency 27 µs at p50 and 1.5 to 2.4 ms at p99, the 10 ms
peak/mean of sends at most 1.11, and the busiest worker 25 % busy. With HTTP/2 over 40
connections (h2c), 200 000 req/s also passed every check (one run):
generator lag 21 µs at p99, corrected latency 73 µs at p50 and 3.2 ms at
p99, busiest worker 45 % busy. The 16-core target of the design has not been
measured on a dedicated client.

## Design measurements

The design left three choices to measurements, made on the machine above.

HTTP client. `cargo run --release -p diapason-proto-http --example
client_overhead` drives 64 keep-alive connections in a closed loop from one
current-thread runtime against the reference server. diapason's writer and
parser cost 6.1 µs of client CPU per request (162 000 req/s on one thread);
hyper 1.x's client connection API costs 8.8 µs (113 000 req/s), 44 % more,
in 2 runs out of 2. hyper's HTTP/1 client also has no pipelining. diapason
keeps its own writer and parser (`httparse` for the response head).

Runtime. `cargo run --release -p diapason-proto-http --example
runtime_overhead` runs the same minimal client loop on tokio's current-thread
runtime (epoll) and on monoio with io_uring: 6.0 µs against 5.56 µs of
client CPU per request (166 000 against 179 000 req/s on one thread), 7 %
less for io_uring. Work that io_uring hands to kernel threads is not counted
in that CPU time. diapason stays on tokio: the gain is small, the throughput
target is met, and io_uring would require owned buffers in the `Protocol`
trait, another timer and another TLS integration, with no io_uring on macOS.

Comparison with wrk2 and Vegeta. `scripts/compare.py --wrk2 PATH --vegeta
PATH` runs the three tools at 2000 req/s with 50 connections, first against
a server that answers at once (each tool's own floor), then against an
M/G/16 queue with exponential service times of mean 1 ms (capacity 16 000
req/s). Corrected latency in ms, 2 and 3 runs:

| server | tool | p50 | p99 | p99.9 |
| --- | --- | --- | --- | --- |
| instant | diapason | 0.13 to 0.14 | 0.21 to 0.28 | 0.28 to 0.33 |
| instant | Vegeta 12 | 0.20 | 0.30 | 0.55 to 0.59 |
| instant | wrk2 | 0.92 to 0.95 | 1.92 to 1.97 | 2.17 to 2.22 |
| M/G/16 | diapason | 0.82 to 0.88 | 4.67 to 4.94 | 6.9 to 7.3 |
| M/G/16 | Vegeta 12 | 0.93 to 0.94 | 4.93 to 4.95 | 7.3 to 7.6 |
| M/G/16 | wrk2 | 1.68 to 1.69 | 5.60 to 5.69 | 7.7 to 8.0 |

The quantiles do not agree within run-to-run noise. The differences between
tools are close to the differences between their floors: wrk2 adds about
0.8 ms everywhere, in line with the 1 ms resolution of its event loop's
timers, and Vegeta about 0.07 to 0.1 ms. diapason's values match the queue:
the median of an exponential of mean 1 ms is 0.69 ms and its 99th percentile
4.61 ms, to which the 0.13 ms floor adds. wrk2 was built from
github.com/giltene/wrk2 at its last commit; its embedded Lua bytecode had to
be generated as a C file (`luajit -b src/wrk.lua obj/bytecode.c`) because the
object file it produces is rejected by the current linker.

## Quick start

```sh
cargo build --release
./target/release/diapason server --listen 127.0.0.1:8080 --capacity 20000 &
./target/release/diapason run --url http://127.0.0.1:8080/ --rate 15000 \
    --arrival poisson --warmup 5s --duration 30s --threads 4 \
    --connections 1000 --summary out.json --trace out.csv.zst
./target/release/diapason selftest
```

The workspace has four crates:

| crate | content |
| --- | --- |
| `diapason-core` | schedules, accounting, histograms, fidelity checks, summary, trace, the worker engine and the `Protocol` trait |
| `diapason-proto-http` | HTTP/1.1 client implementing `Protocol` |
| `diapason-server` | the reference server used by the tests |
| `diapason-cli` | the `diapason` binary (`run`, `server`, `selftest`) and the validation tests |

## The accounting model

### Clock

Every timestamp is read from `CLOCK_MONOTONIC` and stored as nanoseconds
since T0, the start of the run. T0 is fixed 20 ms after all workers have
opened their connections, so connection set-up is not part of any
measurement. The wall-clock time of T0 is recorded once (`started_at`) so
that results can be aligned with telemetry collected elsewhere.

### Life of a request

Each request has up to five timestamps:

| name | meaning |
| --- | --- |
| `intended` | the time the schedule assigned to the request |
| `attempt` | the time the worker's dispatcher handled it |
| `send` | the time the request was handed to the socket (just before the first write) |
| `first_byte` | the time of the read that returned the first byte of the response |
| `done` | the time the last byte of the response was read, or the time of the failure |

From them diapason derives four delays:

| delay | definition | whose delay |
| --- | --- | --- |
| generator lag | `attempt - intended` | the client's scheduling (timer precision, CPU) |
| connection wait | `send - attempt` | waiting for a free connection or in-flight slot |
| server time | `done - send` | network and server |
| corrected latency | `done - intended` | the sum of the three above |

Corrected latency is what a user of the service would have seen had the
request been issued on time. It does not suffer from coordinated omission.

### Outcomes and conservation

Every scheduled request ends in exactly one of four buckets:

- `completed`: a full response was received, whatever its status code. Status
  codes are counted one by one, and `success` counts the completed requests
  whose code is in `http.success_status`;
- `failed`, by kind: the request was sent and no full response came back;
- `not_sent`, by reason: the request was never written to a socket;
- `in_flight_at_end`: the request was sent and had no outcome when the drain
  period ended.

At the end of every run diapason checks

```
scheduled = completed + failed + not_sent + in_flight_at_end
```

where each term is counted from the request's own state: the in-flight count
is the number of requests still held by connections at the end, not a
remainder. A mismatch is a bug in diapason and the run exits with code 4.

Failure kinds:

| kind | cause |
| --- | --- |
| `timeout` | no full response within `timeouts.request` of `send`. Counted once per request. The client then closes the connection |
| `aborted` | the request was pipelined behind a request that timed out, and the connection was closed under it |
| `reset` | the peer reset the connection (`ECONNRESET`, `EPIPE`, `ECONNABORTED`) |
| `read` | the connection was closed or failed while the response was incomplete or not yet started, including the requests pipelined behind a `Connection: close` response |
| `write` | writing the request failed for another reason |
| `protocol` | the response could not be parsed, or arrived when no request was pending |

Connection-level kinds describe failures to open a connection:

| kind | cause |
| --- | --- |
| `dns` | the host name did not resolve. The address is resolved once, at start-up, so in M1 this ends the run with exit code 2 instead of being counted |
| `connect_refused` | the peer refused or reset the attempt (`ECONNREFUSED`, `ECONNRESET`, `ECONNABORTED`) |
| `connect_timeout` | no connection within `connections.connect_timeout` |
| `fd_exhausted` | `socket()` failed with `EMFILE` or `ENFILE` |
| `port_exhausted` | `EADDRNOTAVAIL` or `EADDRINUSE`: no local port left |
| `tls` | the TLS handshake failed (untrusted certificate, name mismatch, rejected client certificate when the server reports it during the handshake) |
| `connect_other` | any other error |

No request is attached to a connection that is not open, so these are counted
in the `connections` block, not as request failures. A request that cannot
find an open connection waits in the client queue and ends as `not_sent`.

Not-sent reasons:

| reason | cause |
| --- | --- |
| `queue` | the request waited in the client queue longer than `connections.max_queue_delay` (checked every 10 ms and whenever a slot frees) |
| `queue_full` | the client queue already held `connections.max_queue_len` requests |
| `end_of_run` | the request was still queued, or assigned to a connection but not yet written, when the drain period ended |

### Phases

A run is a sequence of named phases (for example `warmup`, `measure`,
`cooldown`), each with its own rate profile. A request belongs to the phase
that contains its intended time, whenever it completes, so per-phase
statistics describe what was offered during the phase. The measurement phase
is the one named by `load.measure` (default `measure`); fidelity checks are
computed on it, or on the whole run when no phase has that name.

Two achieved rates are given per phase:

- `achieved_rate_by_intended`: completed requests whose intended time is in
  the phase, divided by the phase duration. Under overload this stays close
  to the offered rate as long as the requests eventually complete;
- `achieved_rate_by_completion`: completed requests whose `done` time is in
  the phase interval, whatever their phase, divided by the phase duration.
  Under overload this is the throughput of the server.

### Schedule

The schedule is defined by a rate profile over the whole run. Let Λ(t) be the
expected number of requests in [0, t). Arrival processes are generated in
Λ-space and mapped back to time through Λ⁻¹, which handles ramps and steps
exactly:

- `constant`: the k-th request of the run is at Λ = k + 0.5;
- `poisson`: gaps in Λ-space are exponential, from a seeded ChaCha8 generator;
- `replay`: intended times are read from a file, one per line, in seconds
  (decimal) since T0; empty lines and lines starting with `#` are skipped.
  The file must not decrease. Phases then only name intervals and their
  rates are ignored; times at or after the end of the last phase are dropped.

The schedule is split across W workers by interleaving. Worker w takes the
constant arrivals k ≡ w (mod W), so consecutive requests of the aggregate go
to different workers and the aggregate is as smooth as the schedule, for any
thread or connection count. For Poisson each worker generates an independent
Poisson process of intensity λ(t)/W; their superposition is a Poisson process
of intensity λ(t). Replay files are split by line number modulo W. A
request's `seq` is `j·W + w`, where j is its rank in worker w; it is unique,
and with constant arrivals it is also the global rank.

Worker w seeds its generator with the master seed and stream number w. The
master seed is recorded in the summary. With the same seed, configuration and
thread count, the sequence of `(seq, intended)` pairs is identical.

### Timer

Each worker waits until `timer.spin` (default 100 µs) before the next
intended time, then yields to its runtime in a loop until the intended time,
so that responses are still processed during the spin. On Linux the wait uses
a `timerfd` armed with an absolute deadline, and the worker's timer slack is
set to 1 ns. Elsewhere one process-wide timer thread keeps the deadlines,
sleeps on a condition variable with a nanosecond timeout and wakes the
worker; on macOS the workers and that thread run at the user-interactive
quality of service, whose timers the kernel does not coalesce. Setting
`DIAPASON_PORTABLE_TIMER=1` uses the timer thread on Linux too: in the
selftest it gave a generator lag of 110 to 122 µs at p99, against 55 to 76 µs
with the `timerfd`. Requests
whose intended time has already passed are dispatched at once, yielding to
the connections every 32 requests. The lag that remains is measured per
request. When intended times are closer together than `spin` (above 10 000
req/s per worker with the default), the worker spins all the time and uses a
full CPU; `--spin 0s` trades about 0.1 ms of lag at p99 for that CPU.

### Concurrency and admission

Three settings are independent:

- `connections.count`: the number of connections opened before T0;
- `connections.pipeline`: the number of requests that may be outstanding on
  one connection (1 means no pipelining);
- `connections.max_in_flight`: the number of requests assigned to
  connections and not yet finished, over the whole client (default
  `max × depth`, where `max` is `connections.max` or `count`, and depth is
  `pipeline` for HTTP/1.1 and `http.h2_max_streams` for HTTP/2);
- `connections.max`: when set above `count`, connect-on-demand. A worker
  opens a new connection when the queued requests outnumber the room of the
  connections it is already opening, and one ahead when a request takes the
  last connection with room, so that the next request does not wait for a
  connect; it stops at `max` connections. In the validation test this kept
  the longest connection wait under 0.5 ms while the pool grew by 100
  connections per second. `count` may then be 0. Connections opened
  this way are never closed by diapason before the end; the cap bounds the
  concurrency pushed into an overloaded server, and the summary reports it
  (`opened_on_demand`, `peak_open`, `connections_open_max` per window).

Connections, in-flight slots and the schedule are split across workers;
remainders go to the first workers, so nothing is dropped (1000 connections
on 104 threads gives 64 workers with 10 and 40 with 9). Each worker owns its
connections. A worker needs at least one connection (counting those it may
open on demand) and one in-flight slot: if `threads` exceeds either, the
thread count is lowered, and the change appears in `config.threads` and in
`warnings`.

When a request is handled and no connection has room, or the in-flight bound
is reached, the request joins the worker's FIFO queue. It leaves the queue
when a slot frees (the wait is its connection wait), or it is dropped as
`not_sent: queue` after `max_queue_delay`. The queue holds at most
`max_queue_len` requests. `max_queue_delay = "0s"` drops at once any request
that finds no free slot. Idle connections are used in round-robin order.

When a connection fails, its pending requests fail, the requests assigned to
it but not yet written go back to the head of the queue (`requeued`), and
the connection reconnects. Connect failures back off from 10 ms, doubling up
to 1 s. Connections that fail before T0 are retried in the same way during
the run.

### Distributed mode

`diapason agent --listen HOST:PORT` (default `0.0.0.0:7700`) waits for a
coordinator on each client node. `diapason run --agents a:7700,b:7700 ...`
is the coordinator: it takes the usual configuration and options, generates
no load itself, and writes the summary, the window file, the interval log and
the live metrics.

The run has W workers in total, numbered globally; `threads` (or the
agent's core count) applies to every agent, and agent k runs a contiguous
range of worker numbers. Each worker takes its share of the schedule, of the
connections and of the in-flight bound from its global number, exactly as in
a one-node run with W threads: with the same seed and configuration, the
requests (seq, intended time, scenario, content) are the same. Agents do
not summarise: they forward their workers' raw windows (histograms, counts,
10 ms slices) and final counters, and the coordinator runs the same
aggregator and builds the same summary as a one-node run, so quantiles,
burstiness and fidelity checks cover all agents together.

Clocks. Before T0 every agent opens its connections and reports ready. The
coordinator then exchanges 32 timestamped messages with each agent and keeps
the exchange with the smallest round trip: the agent's wall-clock offset is
known to within half of that round trip, which the summary gives with the
offset. T0 is chosen at least 100 ms (and four round trips) ahead on the
coordinator's clock and sent to each agent in that agent's own wall clock;
the agent maps it onto its monotonic clock. Every timestamp of an agent is
relative to its own T0, so an error on an offset shifts that agent's load in
real time without showing in its measurements; the offsets and round trips
in the summary bound it. In the validation test, two agents on one host, one
with its wall clock moved 500 ms ahead, got offsets of 0.001 and 500.001 ms
(round trips of 38 µs) and started within 0.1 µs of each other on the shared
monotonic clock.

Traces stay on the agents: each writes its own file, with the name prefixed
by its worker range (`w0-3.out.csv.zst`). `--clock-skew-ms` (hidden) shifts
an agent's wall clock for such tests. If an agent dies or loses its
connection, the coordinator ends with a runtime error (exit 1) naming it; the
other agents finish their schedule. A stop request (`Coordinator::run_with_stop`)
is forwarded to every agent. Agents serve one run at a time and keep
listening afterwards. Messages are JSON preceded by a 4-byte length, over
TCP, without authentication or encryption: run agents on a trusted network.

### Stopping early

A library user can stop a run started with `Runner::spawn` (see Library
use). The schedule then ends at once: requests whose intended time comes
later are never scheduled, so they appear in no count, and the drain starts
immediately. The summary sets `run.stopped_early`.

### End of run

The dispatcher stops after the last intended time before the end of the last
phase. The drain period then starts, at the end of the last phase or when the
dispatcher has handled its last request, whichever is later: queued requests
are still sent and in-flight requests may complete, until everything has an
outcome or `timeouts.drain` has elapsed. Requests still queued, or assigned
but not written, then become `not_sent: end_of_run`, and requests still sent
become `in_flight_at_end`.

### Requests, templates and scenarios

Without `[[scenarios]]`, every request is the one described by `[target]`:
`url`, `method`, `headers`, `body`. With `[[scenarios]]`, each request picks
one scenario with probability proportional to its `weight`; a scenario sets
`method`, `path`, `headers` and `body`, and takes the missing ones from
`[target]` (its headers are added to the target's, replacing those of the
same name). All scenarios go to the host, port and scheme of `target.url`
over the same connections.

Paths, header values and bodies are templates: `{{name}}` is replaced by
`seq` (the request's sequence number), by a variable of `[vars]`, or by
a column of a data file of `[data]` written `file.column`. Unknown names are
configuration errors. A template is compiled once into literal and reference
parts; no interpreter runs per request. Requests without references are
serialised once.

Variables (`dist` selects the distribution):

| `dist` | parameters | value |
| --- | --- | --- |
| `uniform` | `min`, `max` | integer in [min, max] |
| `zipf` | `n`, `s` | integer k in [1, n] with probability proportional to 1/k^s |
| `normal` | `mean`, `stdev`, `round` (default false) | real number, or integer when `round` |
| `choice` | `values`, optional `weights` | one of the values |
| `sequence` | `start` (default 0), `step` (default 1) | start + step × seq |

Data files (`[data.NAME]`) have a `file`, a `format` (`csv` with a header
line, or `jsonl` with one object per line; by default from the extension) and
a `mode`: `sequential` uses row seq mod rows, `random` a row drawn from the
seed and seq, `unique` row seq. In `unique` mode, requests whose seq exceeds
the last row wrap around and are counted in
`protocol_report.data_wrapped.NAME`. Files are read into memory at start-up.

The scenario of a request, the values of its variables and its random data
rows are functions of the master seed and `seq` only, drawn from a
counter-based generator. A variable used twice in one request has the same
value both times. The same seed and configuration therefore give the same
requests, whichever worker or connection sends them and whenever.

A request is rendered when its connection writes it. With pipelining and a
mix of `HEAD` and other methods on one connection, each response is parsed
according to the method of the request it answers.

### HTTP/2

`http.version = "2"` (`--http2`) sends each request on its own stream, up
to `http.h2_max_streams` (default 100) per connection; the server's
SETTINGS limit applies too, and a request waiting for a stream the server
does not grant yet counts that wait in its server time. Over TLS, HTTP/2 is
negotiated with ALPN `h2` (a server that does not select it fails the
connection as `tls`); over cleartext diapason uses prior knowledge (h2c).
Responses may complete in any order. A request timeout cancels that stream
alone (RST_STREAM), counts `timeout` for it and `connections.cancelled`, and
keeps the connection; a stream reset by the server fails that request alone
as `reset`. Connection-level errors and GOAWAY fail the requests in flight on
the connection, as in HTTP/1.1. Connection-specific headers (`connection`,
`keep-alive`, `transfer-encoding`, `upgrade`) are not sent, and `host`
becomes the `:authority` of the URI. `bytes_in` counts body bytes only.
`connections.pipeline` must stay 1.

### TLS

A `https://` URL uses TLS 1.2 or 1.3 through rustls (ring provider) with ALPN
`http/1.1`. Server certificates are checked against the Mozilla roots plus
`tls.ca_file`; `tls.server_name` overrides the name sent in SNI and checked
in the certificate (default: the URL host); `tls.client_cert` and
`tls.client_key` (PEM) authenticate the client; `tls.insecure_skip_verify`
accepts any certificate and is meant for tests. The handshake is part of
connection set-up, before T0 for the initial connections, so it is not part
of any request's latency.

### Closed-loop mode

`mode = "closed"` (`--closed-loop`) keeps every connection busy: a new
request is sent as soon as the previous one on that connection finishes,
until the end of the last phase. There is no schedule, so `intended =
attempt` and generator lag is zero; corrected latency equals server time and
suffers from coordinated omission. This mode exists to compare with
closed-loop tools. It is never the default, the summary says `"mode":
"closed"`, and the text report starts with `CLOSED-LOOP`. Schedule-based
fidelity checks are skipped and listed as skipped.

## Fidelity checks

Fidelity checks say whether the load that reached the target is the load that
was configured. They are computed on the measurement phase. Any failed check
makes the exit code 3. Thresholds are in the `[fidelity]` table.

| check | passes when | default |
| --- | --- | --- |
| `generator_lag_p99` | p99 of generator lag ≤ `lag_p99` | 1 ms |
| `generator_lag_max` | max of generator lag ≤ `lag_max` | 50 ms |
| `send_rate` | requests with `send` in the phase interval, divided by requests scheduled in the phase, ≥ `send_ratio_min` | 0.99 |
| `send_rate_worker_N` | the same ratio for each worker that scheduled at least 100 requests | 0.99 |
| `burstiness_10ms` | peak/mean of sends per 10 ms slice ≤ `burst_factor_10ms` × peak/mean of the schedule, and share of empty slices ≤ that of the schedule + `empty_slack` | 2.0, 0.01 |
| `burstiness_50ms` | the same with 50 ms slices and `burst_factor_50ms` | 1.5, 0.01 |
| `connections` | all connections requested were open at T0 | |
| `admission` | share of scheduled requests that waited more than `conn_wait_threshold` for a slot, or were dropped from the queue, ≤ `admission_share_max`. A failure sets `admission_bound: true` | 1 ms, 1 % |
| `worker_busy_N` | busy share of worker N ≤ `busy_share_max` | 0.9 |
| `worker_runqueue_N` | share of wall time worker N spent runnable but not running ≤ `runqueue_share_max` (Linux only, skipped elsewhere) | 0.05 |

Burstiness compares sends with the schedule itself, so a Poisson schedule is
not penalised for being Poisson. Only slices that lie entirely inside the
measurement interval are counted. At high rates a 10 ms slice holds thousands
of requests and a stall of a few milliseconds shared by all workers moves
enough of them into the next slice to reach a peak/mean of 1.5; the 10 ms
factor is therefore looser than the 50 ms one.

User thresholds (`[[thresholds]]` with `expr` and an optional `phase`,
default the measurement phase, or `--threshold EXPR`) are assertions on the
phase summary, checked after the run; any failure makes the exit code 5.
They never replace the fidelity checks. The grammar is `METRIC OP RHS`, with
OP one of `<`, `<=`, `>`, `>=`, and RHS a number (with `ns`, `us`, `ms`, `s`
for latencies, `%` for ratios), a metric, or `NUMBER * METRIC`. Metrics:
`[KIND.]STAT` with KIND `corrected` (default), `server`, `connection_wait`
or `generator_lag` and STAT `min`, `mean`, `p50`, `p90`, `p99`, `p99.9`,
`p99.99` or `max`; `offered_rate`, `nominal_rate`, `achieved_rate` (by
intended time), `achieved_rate_by_completion`; `success_ratio`,
`error_rate` (1 − success_ratio), `failed_rate` and `not_sent_rate` (over
scheduled requests). Examples: `p99 < 200ms`, `server.p99.9 <= 1s`,
`error_rate < 0.1%`, `achieved_rate >= 0.99 * offered_rate`.

Busy share is the time the worker spent polling diapason's own tasks
(dispatching, connection I/O, accounting) over wall time. It excludes the
dispatcher's timer spin and the runtime's event loop, so it is a lower bound
of the CPU the worker needed. Runqueue share comes from
`/proc/self/task/<tid>/schedstat`; a high value means the client was starved
of CPU by other threads or processes.

## Output

### Text summary

Printed on stdout at the end of `diapason run` (on stderr with `--summary
-`). Its content mirrors the JSON summary; it is for humans and its layout may
change. Use the JSON for anything automatic.

### JSON summary (`--summary`, schema `diapason.summary` version 1)

Durations in the summary are in milliseconds (keys end in `_ms`) or seconds
(keys end in `_s`); rates are in requests per second.

Top level:

| field | meaning |
| --- | --- |
| `schema`, `schema_version` | `"diapason.summary"`, `1` |
| `tool` | `name`, `version`, `git_hash` (null when built outside a git checkout with commits) |
| `mode` | `"open"` or `"closed"` |
| `seed` | master seed |
| `started_at` | wall-clock time of T0: `unix_ns` and `rfc3339` (UTC) |
| `host` | see below |
| `config` | the configuration as used, after defaults and command-line overrides, in the same shape as the TOML file; `seed`, `threads` and `connections.max_in_flight` are filled in |
| `run` | `scheduled_duration_s` (end of last phase), `wall_duration_s` (T0 to the end of the drain), `threads`, `measure_phase` (name or null), `stopped_early`, `protocol` (`http/1.1`, `https/1.1`, `h2c` or `h2`, then the address) |
| `conservation` | see below |
| `phases` | one object per phase, see below |
| `connections` | see below |
| `workers` | one object per worker, see below |
| `fidelity` | see below |
| `thresholds` | one object per user threshold: `expr`, `phase`, `value`, `limit`, `ok` |
| `protocol_report` | protocol-specific counters, or null. HTTP: `data_wrapped` (object data file → requests that wrapped around in `unique` mode) |
| `outputs` | see below |
| `warnings` | list of strings: limits hit, adjustments made |
| `distributed` | only for a distributed run: `agents`, one object per agent with `address`, `host` (as above), `protocol`, `first_worker`, `threads`, `clock_offset_ms` (agent clock minus coordinator clock), `rtt_ms` (the round trip the offset comes from; the offset is known to within half of it), `warnings`, `trace_rows`, `t0_monotonic_ns` (the agent's monotonic clock at T0, comparable only between agents on one host) |
| `exit_code`, `exit_reason` | the process exit code and why |

`host`: `hostname`, `os`, `arch`, `kernel` (release string), `cores` (logical
CPUs available to the process), `fd_soft_initial`, `fd_soft`, `fd_hard` (the
`RLIMIT_NOFILE` values before and after raising), `ephemeral_ports` (`[low,
high]` from `/proc/sys/net/ipv4/ip_local_port_range`, null elsewhere).

`conservation`: `scheduled`, `completed`, `failed` (object kind → count,
non-zero kinds only), `failed_total`, `not_sent` (object reason → count),
`not_sent_total`, `in_flight_at_end`, `balance` (`scheduled` minus the sum of
the four buckets, 0 when the books balance) and `ok`.

Each element of `phases`:

| field | meaning |
| --- | --- |
| `name`, `start_s`, `end_s`, `duration_s` | the phase interval on the intended-time axis |
| `measured` | true for the measurement phase |
| `nominal_rate` | mean rate of the profile over the phase (0 in closed-loop and replay modes) |
| `scheduled` | requests with intended time in the phase |
| `offered_rate` | `scheduled / duration_s` |
| `completed`, `success`, `failed`, `not_sent`, `in_flight_at_end` | the conservation buckets restricted to the phase |
| `status` | object status code → count |
| `achieved_rate_by_intended`, `achieved_rate_by_completion` | see Phases |
| `success_ratio` | `success / scheduled` |
| `latency` | `corrected`, `server`, `connection_wait`, `generator_lag`, each with `count`, `min_ms`, `mean_ms`, `stdev_ms`, `p50_ms`, `p90_ms`, `p99_ms`, `p99_9_ms`, `p99_99_ms`, `max_ms`. `corrected` and `server` cover completed requests, `connection_wait` every request that was sent, `generator_lag` every request the dispatcher handled. Histograms keep 3 significant digits |
| `in_flight` | over the phase interval, all phases' requests mixed: `sent_mean` (time average of requests written and not finished), `system_mean` (the same plus queued requests), `sent_max`, `system_max` (sums of each worker's maximum, so upper bounds) |
| `scenarios` | one object per scenario (one named `default` without `[[scenarios]]`): `name`, `weight_share` (configured weight over the sum), `share` (realised share of the phase's scheduled requests), `scheduled`, `completed`, `success`, `failed`, `not_sent`, `in_flight_at_end` |
| `little` | Little's law over the phase interval: `departure_rate` (requests per second that left the sent state, completed or failed), `mean_time_in_flight_s` (their mean `done - send`), `predicted_in_flight` (the product), `measured_in_flight` (`in_flight.sent_mean`), `relative_error` (null when nothing departed). Requests still in flight at the edges of the interval make the error grow when the system is not stationary, for example under overload |

`connections`: `requested`, `established` (open at T0), `shortfall` (object
kind → number of connections that failed before T0), `connect_attempts`,
`connect_errors` (object kind → count, over the whole run), `reconnects`
(successful connects after T0), `closes` (`server`: the server closed or
answered `Connection: close`; `client`: diapason closed after a timeout;
`error`: closed after a failure), `requeued`, `opened_on_demand`
(connections opened after T0 because requests were waiting), `peak_open`
(sum over workers of the most connections each had open at once, so an
upper bound), `cancelled` (requests abandoned alone after a timeout, their
HTTP/2 connection kept), `port_range_size`.

Each element of `workers`: `id`, `connections` (at the end, counting those
opened on demand), `max_in_flight`, `scheduled`
and `sent` (measurement interval), `send_ratio`, `wall_s` (T0 to the worker's
end), `busy_share`, `spin_share` (share of wall time in the timer spin, during
which other tasks still run), `cpu_s` (thread CPU time), `runqueue_s` and
`runqueue_share` (Linux), `involuntary_switches`.

`fidelity`: `ok`, `admission_bound`, `checks` (list of `name`, `ok`,
`skipped`, `value`, `limit`, `detail`), and the measured inputs:
`generator_lag` (`p99_ms`, `max_ms`), `send_rate` (`scheduled`, `sent`,
`ratio`), `burstiness` (for `slice_10ms` and `slice_50ms`: `schedule` and
`sends`, each with `slices`, `mean`, `peak`, `peak_to_mean`, `empty_share`),
`admission` (`over_threshold`, `share`).

`outputs`: `summary`, `histogram_log`, `windows`, `trace` (paths or null),
`windows_written`, `trace_rows`, `trace_dropped`, `trace_sample`, `metrics`
(the address of the live endpoint, or null).

### Interval histogram log (`--histogram-log`)

HdrHistogram interval-log format (V2, compressed). The tests read it back
with the Rust HdrHistogram crate; other HdrHistogram tools have not been
tried. One interval per window
(`output.window`, default 1 s, a multiple of 50 ms) and per metric, tagged
`corrected`, `server`, `connection_wait` and `generator_lag`. Values are in
nanoseconds; the log's max-value divisor is 10⁶, so tools display
milliseconds. A value is recorded in the window that contains the time of its
event: `attempt` for generator lag, `send` for connection wait, `done` for
corrected latency and server time. StartTime and BaseTime are the wall-clock
time of T0.

### Window file (`--windows`)

One JSON object per line and per window: `index`, `start_s`, `end_s`,
`scheduled` (by intended time), `sent`, `completed`, `failed`, `not_sent` (by
time of the event), `in_flight_mean`, `in_flight_max`, `in_system_mean`,
`in_system_max` (as in the phase `in_flight` block), `connections_open_max`
(sum over workers of the most connections each had open in the window),
and `corrected_p50_ms`,
`corrected_p99_ms`, `server_p99_ms`, `connection_wait_p99_ms`,
`generator_lag_p99_ms` for the window's histograms. With `--progress` part
of it is printed on stderr as the run goes. Library users receive it as a
`WindowSnapshot`.

### Trace (`--trace`)

One row per request: CSV, compressed with zstd when the path ends in `.zst`,
or the binary columnar format described below when it ends in `.dtr`.
Columns: `seq`, `worker`, `conn` (connection index in the worker, empty if
never assigned), `phase` (index), `scenario` (index in `[[scenarios]]`, 0
without scenarios), `intended_ns`,
`attempt_ns`, `send_ns`, `first_byte_ns`, `done_ns` (empty when unknown),
`status` (empty if no response), `bytes_in` (response bytes, headers
included), `error_kind` (empty for a completed request, a failure kind,
`not_sent:<reason>`, or `in_flight_at_end`).

The trace is written by a separate thread through bounded buffers (64
batches of 4096 rows), so its memory does not depend on run length. If the
writer falls behind, rows are dropped and counted in
`outputs.trace_dropped`. `--trace-sample k` keeps one request in k, chosen by
a hash of `seq`, so the same requests are kept across runs with the same seed.

The binary columnar format (`.dtr`) holds the same columns. After a header
(the 8 bytes `DIAPTRC1`, a little-endian u32 version, 1, and a little-endian
u32 length followed by that many bytes of JSON naming the columns and
outcomes) come blocks of up to 4096 rows: a little-endian u32 row count, a
little-endian u32 byte count, and that many bytes of zstd data. Decompressed,
a block holds its columns one after the other, one unsigned LEB128 varint per
row and column: `seq`, `worker`, `phase`, `scenario` and `bytes_in` as they
are; `conn` as 0 when absent, else conn + 1; `intended_ns` as the zigzag
encoding of its difference with the previous row of the block (the first
row: with 0); `attempt_ns`, `send_ns`, `first_byte_ns` and `done_ns` as 0
when absent, else zigzag(value − intended_ns) + 1; `status` as 0 when
absent, else the code; then one byte per row for the outcome, an index into
`["", "timeout", "aborted", "reset", "read", "write", "protocol",
"not_sent:queue", "not_sent:queue_full", "not_sent:end_of_run",
"in_flight_at_end"]`. zigzag(x) is 2x for x ≥ 0 and −2x − 1 otherwise.
`diapason trace FILE.dtr [-o OUT.csv[.zst]]` converts it to the CSV format,
and `diapason_core::dtr::read` decodes it in Rust. At 50 000 req/s (Poisson,
500 000 requests) a trace took 14.3 bytes per request in this format, 27.9 in
CSV with zstd and 78 in plain CSV; converting it back took 0.28 s.

### Live metrics (`--metrics ADDR`)

An HTTP endpoint on `ADDR` (`output.metrics`) answers any request with the
OpenMetrics text format (`application/openmetrics-text; version=1.0.0`):
`diapason_info` (mode, seed, version), `diapason_window_end_seconds`, the
counters `diapason_requests_{scheduled,sent,completed,failed,not_sent}_total`
(summed over the published windows, by time of the event), the gauges
`diapason_in_flight`, `diapason_in_system` and `diapason_connections_open`
of the last window, and `diapason_window_latency_seconds{kind, quantile}`
for the four delays at 0.5, 0.9, 0.99 and 0.999 in the last window. A window
is published once every worker has reported it, so the values trail real
time by one to two windows. The endpoint stops with the run.

### Exit codes

| code | meaning |
| --- | --- |
| 0 | run completed, books balanced, fidelity checks passed |
| 1 | runtime error |
| 2 | configuration error, including a file-descriptor limit too low for the configured connections |
| 3 | a fidelity check failed |
| 4 | conservation broken |
| 5 | a user threshold failed |

When several apply, the first in the order 1, 4, 3, 5 wins.

## Configuration

TOML, with unknown keys rejected. Durations are strings with a unit: `ns`,
`us`, `ms`, `s`, `m`, `h`.

```toml
mode = "open"          # "open" or "closed"
seed = 42              # omitted: drawn from the OS and recorded
threads = 4            # omitted: number of logical CPUs

[target]
url = "http://127.0.0.1:8080/"
method = "GET"
headers = { "x-test" = "1" }
body = ""

[load]
arrival = "poisson"    # "constant", "poisson" or "replay"
# replay_file = "times.txt"
measure = "measure"

[[load.phases]]
name = "warmup"
duration = "10s"
rate = 1000

[[load.phases]]
name = "measure"
duration = "60s"
rate = 1000
rate_end = 5000        # optional: linear ramp from rate to rate_end

# a phase may instead give steps (its duration is their sum) or points:
# steps = [{ duration = "10s", rate = 100 }, { duration = "10s", rate = 200 }]
# points = [{ at = "0s", rate = 100 }, { at = "30s", rate = 900 }]  # piecewise linear, constant after the last point

[connections]
count = 100
# max = 2000           # connect-on-demand up to this many
pipeline = 1
# max_in_flight = 100  # default count × pipeline
max_queue_delay = "60s"
max_queue_len = 1000000
connect_timeout = "5s"

[timeouts]
request = "60s"
drain = "10s"

[timer]
spin = "100us"

[http]
success_status = ["200-399"]  # ranges, single codes, or "2xx"
version = "1.1"                # or "2"
h2_max_streams = 100

[fidelity]
lag_p99 = "1ms"
lag_max = "50ms"
send_ratio_min = 0.99
burst_factor_10ms = 2.0
burst_factor_50ms = 1.5
empty_slack = 0.01
conn_wait_threshold = "1ms"
admission_share_max = 0.01
busy_share_max = 0.9
runqueue_share_max = 0.05

[output]
window = "1s"
summary = "out.json"
histogram_log = "out.hlog"
windows = "windows.jsonl"
trace = "out.csv.zst"
trace_sample = 1
# metrics = "127.0.0.1:9100"

[system]
fd_margin = 64
pin_threads = false

[tls]
# ca_file = "ca.pem"
# client_cert = "client.pem"
# client_key = "client.key"
# server_name = "api.example.org"
insecure_skip_verify = false

[vars]
user = { dist = "zipf", n = 100000, s = 1.1 }
qty = { dist = "uniform", min = 1, max = 5 }

[data.products]
file = "products.csv"   # header line: sku,price
mode = "random"

[[scenarios]]
name = "browse"
weight = 9
path = "/users/{{user}}/feed"

[[scenarios]]
name = "buy"
weight = 1
method = "POST"
path = "/cart"
headers = { "content-type" = "application/json", "x-request" = "{{seq}}" }
body = '{"user": {{user}}, "sku": "{{products.sku}}", "qty": {{qty}}}'

[[thresholds]]
expr = "p99 < 200ms"

[[thresholds]]
expr = "error_rate < 0.1%"
phase = "measure"
```

At start-up diapason raises `RLIMIT_NOFILE` to the hard limit and refuses to
start (exit 2) if the most connections the run may open (`connections.max`,
or `count`) plus `fd_margin` plus 16 per thread exceeds it. It warns when that
number exceeds the ephemeral port range.

## Command line

```
diapason run [--config FILE] [options]
diapason server [options]
diapason selftest [--rate R] [--duration D]
diapason agent [--listen HOST:PORT]
diapason trace FILE.dtr [-o OUT.csv]
```

`diapason run` options override the file: `--url`, `--method`, `-H/--header
"name: value"`, `--body`, `--arrival`, `--replay-file`, `--threads`,
`--connections`, `--pipeline`, `--max-in-flight`, `--max-queue-delay`,
`--max-queue-len`, `--connect-timeout`, `--timeout`, `--drain`, `--seed`,
`--closed-loop`, `--summary FILE` (`-` for stdout), `--trace`,
`--trace-sample`, `--histogram-log`, `--windows`, `--window`, `--spin`,
`--pin`, `--progress`, `-q/--quiet`, `--threshold EXPR` (repeatable),
`--insecure`, `--ca-file`, `--client-cert`, `--client-key`,
`--max-connections`, `--http2`, `--metrics ADDR`, `--agents LIST`. Any key can be set with `--set
key=value`, for example `--set fidelity.lag_p99=2ms`. When `--rate`,
`--warmup`, `--duration` or `--cooldown` is given, the phases are replaced by
`warmup` (if given), `measure` (default 10 s) and `cooldown` (if given), all at
`--rate`.

`diapason selftest` starts the reference server in the process (capacity 8000
req/s), runs 1 s of warmup and 3 s of measurement at 2000 req/s, and checks
that every request completed, the books balance and the fidelity checks pass.

## Library use

```rust
use diapason_core::{RunConfig, Runner};
use diapason_proto_http::HttpProtocol;

let cfg = RunConfig::from_toml(&std::fs::read_to_string("run.toml")?)?;

// Blocking: run on this thread.
let summary = Runner::new(cfg.clone(), HttpProtocol::from_config(&cfg)?)
    .on_window(|w| eprintln!("window {}: {} completed", w.index, w.completed))
    .run()?;
assert!(summary.conservation.ok);

// On a thread, with a window channel and an early stop.
let (runner, windows) = Runner::new(cfg.clone(), HttpProtocol::from_config(&cfg)?).window_channel();
let handle = runner.spawn();
for w in windows {
    let h = &w.hists.corrected;
    if !h.is_empty() && h.value_at_quantile(0.99) > 50_000_000 {
        handle.stop(); // p99 above 50 ms in this window
    }
}
let summary = handle.join()?;
```

`Summary` is the typed form of the JSON summary and `WindowSnapshot` carries
the counts, gauges and histograms (in nanoseconds) of one window. A new
protocol implements `diapason_core::Protocol`: `connect` returns a writer
half and a reader half, the writer writes one request (it receives the
request's `seq` and `scenario`), the reader reads one response and must be
cancel-safe (the engine drops a pending read when a timeout fires). A
multiplexed protocol returns `max_outstanding`, tags each response with the
request's `seq`, reports single-request failures in `Response::fail`, and
implements `cancel`; tasks it starts should go through
`diapason_core::spawn_task` so that the worker's busy share counts them.
`set_seed` receives the master seed before the run and `report` returns the
protocol's counters. The templates of `diapason_core::template` are
available to any protocol. The scheduling and accounting code does not
change.

The crate root's exports and the public modules (`clock`, `config`,
`duration`, `hist`, `host`, `protocol`, `report`, `summary`, `template`,
`threshold`) are the public API. In a 0.x version a minor version may break
it and a patch version does not. The JSON summary has its own
`schema_version`, raised when a field changes meaning or disappears; fields
may be added without raising it.

`Coordinator::new(cfg, agents).run()` runs the same configuration on
agents and returns the same `Summary`, with its `distributed` block;
`serve_agent(listen, make, 0, on_listen)` turns any program into an agent
for the protocol that `make` builds from the configuration.

`cargo run --release -p diapason-cli --example rate_search` finds the
highest rate whose p99 stays under 10 ms on the reference server by
bisection. Each step runs with `spawn`, watches the window channel and stops
after three windows over the target; a step counts only when the fidelity
checks passed. On a server of capacity 8000 req/s (M/M/8, mean service
1 ms) it reported about 7400 req/s: p99 8.5 ms at 7406 req/s, 10.2 ms at
7719 req/s.

## Validation

`cargo test --release -p diapason-cli` runs the validation tests against
`diapason server` (see `crates/diapason-cli/tests/validation.rs`), one at a
time, in about 2 minutes:

1. below capacity, achieved rate equals offered rate within 0.5 % and
   corrected latency is within 0.5 ms of server time at p50 and p99;
2. at r = 1.1 μ with enough connections, corrected latency grows with slope
   r/μ − 1 within 10 % (least squares on the trace) and throughput is within
   2 % of μ;
3. with one connection, connection wait grows, the run is admission-bound
   (exit 3), and the mean corrected latency equals the sum of the mean lag,
   wait and server time within 1 %;
4. an fd limit (256) below the requested connections (1000) makes diapason
   refuse to start (exit 2);
5. 64 threads × 4032 connections at 220 req/s give a 50 ms peak/mean below 2
   and no empty slice;
6. conservation holds with server resets, closes and stalls, and with the
   server killed in the middle of the run;
7. a request that times out is counted once (40 scheduled, 40 timeouts, 40
   client closes);
8. a client pinned to one core at 150 000 req/s fails its fidelity checks
   (exit 3);
9. the same seed and configuration give the same `(seq, intended)` sequence,
   and another seed a different one.

A further test reads the interval log back with the HdrHistogram library and
checks the window file and the trace against the summary. The M2 tests check
that a failed threshold gives exit code 5 and passing ones 0, that the
realised scenario mix is within 2 % of the weights with statuses matching the
rendered paths, that HEAD and GET mixed on pipelined connections all
complete, and that HTTPS works with a private CA, an SNI override and client
certificates, fails without the client certificate, and counts `tls` connect
errors without the CA. The M3 tests check that connect-on-demand follows the
backlog above capacity (connections open grow in every window, all reported)
and that a low cap makes the run admission-bound with exactly the cap open;
that h2c carries all the streams on one connection with more than one in
flight, and h2 works over TLS with ALPN; that HTTP/2 stream resets and
timeouts fail one request each without reconnecting; that a run spawned
through the library and stopped after two windows ends early with balanced
books; and that the live endpoint serves OpenMetrics during a run. The M4
tests run two agents on one host, one with its clock moved 500 ms ahead:
the run balances, sends exactly the requests of a one-node run with the same
number of workers, measures both offsets within 1 ms and aligns T0 within
1 ms; and an agent killed during a run ends the coordinator with exit code 1
in bounded time. A last test checks that a binary trace, converted back with
`diapason trace`, holds the same requests as the CSV trace of a run with the
same seed.

The reference server (`diapason server`) is an M/G/c queue in virtual time:
`--workers c` servers, a service-time distribution (`--service const:1ms`,
`exp:1ms`, `uniform:1ms:3ms`, `lognormal:1ms:0.5`, where the first value is
the mean), so the capacity is μ = c / E[S]; `--capacity μ` sets c from μ.
Each request is assigned on arrival to the server that frees first, and its
response is written when its service ends: at once if it is already due,
otherwise when a dedicated timer thread (condition variable with a
nanosecond timeout, timer slack 1 ns) wakes the connection. On localhost the
round trip through a server that answers at once is about 0.13 ms at p50;
with a constant 1 ms service the p50 was 1.16 ms, so the wake-up added about
30 µs. `--slowdown at:for:factor`
multiplies service times for services starting in an interval measured from
server start; `--reset-prob`, `--close-prob` and `--stall-prob` make a
request end in a connection reset, a `Connection: close` response, or no
answer at all. `--workers 0` answers at once. A path `/status/NNN` gets
status NNN; HEAD requests get headers only. The server speaks HTTP/2 when a
cleartext connection starts with the HTTP/2 preface or when TLS negotiates
`h2`; there `--reset-prob` resets a stream, `--stall-prob` leaves it
unanswered, and `--close-prob` does not apply. `--tls-cert` and `--tls-key`
serve TLS, and `--tls-client-ca` requires client certificates signed by that
CA. The server prints `listening
ADDR (...)` on stdout when ready and its counters as JSON on stderr when it
receives SIGINT or SIGTERM.
