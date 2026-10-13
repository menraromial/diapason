# Changelog

All notable changes to diapason are listed here. Versions follow
[Semantic Versioning](https://semver.org/); before 1.0, a minor version may
break the library API, the configuration or the summary schema (the summary
carries its own `schema_version`).

## 0.1.0 (2026-10-13)

First release.

- Open-loop scheduling: constant, Poisson, ramp, step and piecewise profiles,
  replay files, named phases; the schedule is split across workers by
  interleaving, so the aggregate stays smooth for any thread and connection
  count.
- Per-request accounting of generator lag, connection wait, server time and
  corrected latency, with a conservation check at the end of every run
  (exit code 4 if it fails).
- Fidelity checks (generator lag, send rate, burstiness in 10 ms and 50 ms
  slices, connections, admission, worker busy and run-queue shares), exit
  code 3 when one fails; user thresholds, exit code 5.
- HTTP/1.1 with keep-alive and pipelining, HTTP/2 (h2 and h2c) with a stream
  limit, TLS with rustls (SNI, client certificates, CA files).
- Bounded connection pool, admission bound, client queue with a delay limit,
  and connect-on-demand up to a cap.
- Request templates with variables (uniform, Zipf, normal, choice, sequence)
  and CSV or JSON-lines data files; weighted scenarios with the realised mix.
- Outputs: text summary, JSON summary (schema `diapason.summary`, version 1),
  HdrHistogram interval log, window file, per-request trace in CSV (optionally
  zstd) or binary columnar format, live OpenMetrics endpoint.
- Closed-loop mode, labelled as such, for comparison with other tools.
- Distributed mode: a coordinator and agents, clock-offset measurement,
  merged histograms; the same requests as a one-node run with the same seed.
- Library API: `Runner` (blocking or spawned, window channel, early stop),
  `Coordinator`, the `Protocol` trait for other protocols.
- Reference server: an M/G/c queue in virtual time with fault injection,
  HTTP/1.1, HTTP/2 and TLS; validation tests against it in CI.
- Linux is the platform for precise measurements (timerfd, generator lag of a
  few microseconds); on macOS the generator lag is about a millisecond.
