---
id: live-metrics
title: Live metrics
sidebar_label: Live metrics
description: Watch a long run with the OpenMetrics endpoint, curl and Prometheus.
---

For long runs, `--metrics ADDR` serves the progress in the OpenMetrics text
format while the run goes on.

```bash
diapason run --url http://127.0.0.1:8080/ --rate 1000 --duration 10m \
  --metrics 127.0.0.1:9100
```

```bash
curl -s http://127.0.0.1:9100/metrics | grep -v '^# '
```

```text
diapason_info{mode="open",seed="10884265606090315919",version="0.1.0"} 1
diapason_requests_scheduled_total 3029
diapason_requests_completed_total 3029
diapason_requests_failed_total 0
diapason_in_flight{stat="mean"} 1.395718664
diapason_in_flight{stat="max"} 9
diapason_connections_open{stat="max"} 20
diapason_window_latency_seconds{kind="corrected",quantile="0.99"} 0.005636095
# EOF
```

The counters add up the published windows; the gauges and quantiles describe
the last window. A window is published once every worker has reported it,
so the values trail real time by one or two windows.

For a quick look in the terminal, `--progress` prints one line per window on
stderr instead.

## Prometheus

```yaml title="prometheus.yml"
scrape_configs:
  - job_name: diapason
    scrape_interval: 1s
    static_configs:
      - targets: ["127.0.0.1:9100"]
```

Useful queries:

| query | shows |
| --- | --- |
| `rate(diapason_requests_completed_total[10s])` | completions per second |
| `rate(diapason_requests_failed_total[10s])` | failures per second |
| `diapason_window_latency_seconds{kind="corrected",quantile="0.99"}` | p99 of the last window |
| `diapason_window_latency_seconds{kind="generator_lag",quantile="0.99"}` | whether the client keeps up |
| `diapason_in_flight{stat="mean"}` | concurrency seen by the server |

The live metrics are for watching; the summary, windows and histogram log
written at the end are the record of the run.
