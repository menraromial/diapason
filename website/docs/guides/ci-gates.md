---
id: ci-gates
title: CI performance gates
sidebar_label: CI performance gates
description: Thresholds on latency, errors and throughput, exit codes, and a GitHub Actions job.
---

diapason does not decide whether a service is fast enough: you do, with
thresholds. A failed threshold makes the run exit with code 5, apart from
the fidelity checks (code 3) that say whether the measurement itself can be
trusted.

## Thresholds

On the command line, repeatable, on the measurement phase:

```bash
diapason run --url http://127.0.0.1:8080/ --rate 500 --duration 10s \
  --threshold "p99 < 50ms" \
  --threshold "server.p99.9 <= 200ms" \
  --threshold "error_rate < 0.1%" \
  --threshold "achieved_rate >= 0.99 * offered_rate"
```

Or in the configuration, with an optional phase:

```toml
[[thresholds]]
expr = "p99 < 50ms"

[[thresholds]]
expr = "error_rate < 0.1%"
phase = "measure"
```

The report lists them after the fidelity checks:

```text
thresholds:
  FAIL error_rate < 1% on measure: value 0.0494, limit 0.0100
exit 5: thresholds failed: error_rate < 1%
```

An expression is `METRIC OP VALUE`: latency statistics such as `p50`,
`p99`, `p99.9`, `max` or `mean` (of corrected latency, or prefixed with
`server.`, `connection_wait.` or `generator_lag.`) with a unit; rates such
as `achieved_rate` and `offered_rate`; ratios such as `error_rate`,
`success_ratio` and `failed_rate`, as a number or a percentage. The right
side can be another metric times a factor. A malformed expression is a
configuration error (code 2).

## Exit codes in a pipeline

| code | meaning | what to do in CI |
| --- | --- | --- |
| 0 | measurement trusted, thresholds met | pass |
| 3 | the client did not deliver the load: the numbers are not about the server | fail, or rerun on a quieter runner |
| 5 | thresholds failed | fail: the service is slower than allowed |
| 2 | configuration error | fail |
| 1, 4 | runtime error, conservation broken | fail and report |

Shared CI runners are noisy. Keep the rate modest, give the client enough
CPU, and look at the fidelity checks before blaming the service.

## A GitHub Actions job

```yaml title=".github/workflows/perf.yml"
name: performance
on: [pull_request]
jobs:
  perf:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install diapason
        run: |
          curl -fsSL https://raw.githubusercontent.com/menraromial/diapason/main/install.sh | sh
          echo "$HOME/.local/bin" >> "$GITHUB_PATH"
      - name: Start the service under test
        run: ./scripts/start-service.sh &   # your service
      - name: Load test
        run: |
          diapason run --url http://127.0.0.1:8000/health --rate 500 \
            --warmup 5s --duration 30s --threads 2 --connections 50 \
            --threshold "p99 < 50ms" --threshold "error_rate < 0.1%" \
            --summary perf.json
      - name: Keep the summary
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: perf-summary
          path: perf.json
```

`jq '.thresholds'` or `jq '.fidelity.checks[] | select(.ok | not)'` on the
summary give the details of a failure.
