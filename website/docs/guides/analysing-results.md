---
id: analysing-results
title: Analysing results
sidebar_label: Analysing results
description: The JSON summary with jq, the window file, HdrHistogram logs, and per-request traces with pandas.
---

diapason writes four kinds of output, from the most condensed to the most
detailed. All of them come from the same run:

```bash
diapason run --url http://127.0.0.1:8080/ --rate 3000 --warmup 2s --duration 10s \
  --threads 2 --connections 50 --seed 1 \
  --summary run.json --windows windows.jsonl --histogram-log run.hlog --trace run.dtr
```

| file | what | size here |
| --- | --- | --- |
| `run.json` | the summary: configuration, host, per-phase statistics, fidelity, conservation | one object |
| `windows.jsonl` | one line per second: counts, in flight, quantiles | 13 lines |
| `run.hlog` | HdrHistogram interval log, 4 histograms per second | readable by HdrHistogram tools |
| `run.dtr` | one row per request, binary columnar | 604 KB for 36 187 requests |

## The summary with jq

```bash
jq '.phases[] | select(.measured) | {offered_rate, achieved_rate_by_completion,
    p99_ms: .latency.corrected.p99_ms, server_p99_ms: .latency.server.p99_ms}' run.json
```

```json
{
  "offered_rate": 3014.9,
  "achieved_rate_by_completion": 3015.0,
  "p99_ms": 4.849663,
  "server_p99_ms": 4.841471
}
```

```bash
jq '{exit_code, exit_reason, conservation: .conservation.ok, seed}' run.json
jq -r '.fidelity.checks[] | select(.skipped | not) | "\(if .ok then "ok  " else "FAIL" end) \(.name)"' run.json
```

The summary also records the configuration as used (`.config`, with the
seed and thread count filled in), the host (`.host`: kernel, cores,
file-descriptor limits) and the tool version with its git commit (`.tool`),
so a result can be traced back to how it was produced. Its schema is
versioned (`schema_version`); every field is described in
[Outputs](../outputs).

## Windows: the run over time

```bash
jq -r '[.start_s, .completed, .in_flight_mean, .corrected_p99_ms] | @tsv' windows.jsonl | head -4
```

```text
0.0	2966	3.513493748	4.657151
1.0	3068	3.702486166	5.046271
2.0	2965	3.595348846	5.038079
3.0	2938	3.61821966	4.784127
```

Load it into a spreadsheet or a notebook to plot throughput, in-flight
requests, open connections or p99 against time. `--window 500ms` changes
the window length (a multiple of 50 ms).

## Interval histograms

`run.hlog` is the HdrHistogram interval-log format, tagged `corrected`,
`server`, `connection_wait` and `generator_lag`, values in nanoseconds:

```text
#[StartTime: 1791922419.334 (seconds since epoch)]
#[BaseTime: 1791922419.334 (seconds since epoch)]
#[MaxValueDivisor: 1000000.000]
Tag=corrected,0.000,1.000,9.159,HISTFAAABZd4nE1XvaokVRA+U6fOOd3TDs0wDMvlernIssiygQ8gYiAGi4EYGBssIgY+
```

HdrHistogram libraries (Java, Rust, Python, Go) read it, so histograms can be
merged across windows or runs without losing precision.

## Per-request traces

A path ending in `.dtr` writes the compact binary format (about 14 bytes per
request), `.csv.zst` compressed CSV, anything else plain CSV. Convert a
binary trace to CSV:

```bash
diapason trace run.dtr -o run.csv
```

Every scheduled request has a row: its sequence number, worker, connection,
phase, scenario, the five timestamps (ns since the start), the status, the
bytes received, and its outcome. With pandas:

```python
import pandas as pd

t = pd.read_csv("run.csv")
done = t[t.error_kind.isna()]                      # completed requests
lat = (done.done_ns - done.intended_ns) / 1e6      # corrected latency, ms
print(len(t), "requests,", len(done), "completed")
print("p50 %.3f  p99 %.3f" % (lat.quantile(0.5), lat.quantile(0.99)))

# p99 per second of intended time
per_s = done.assign(sec=done.intended_ns // 1_000_000_000, lat=lat)
print(per_s.groupby("sec").lat.quantile(0.99).round(2).head())
```

```text
36187 requests, 36187 completed
p50 0.893  p99 4.842
sec
0    4.65
1    5.03
2    5.04
3    4.78
4    4.75
```

The p99 from the trace (4.842 ms) matches the summary's (4.850 ms) within
the 3 significant digits of the histograms. `--trace-sample 10` keeps one
request in ten, chosen by a hash of `seq`, so the same requests are kept
across runs with the same seed.

## From Rust

The library returns the summary as a typed struct and hands every window to
a callback or a channel; see [Library](../library).
