---
id: realistic-requests
title: Realistic requests
sidebar_label: Realistic requests
description: Weighted scenarios, templated paths, headers and bodies, variables drawn from distributions, and data files.
---

Real traffic is not one URL hit at a fixed rate: users browse more than they
buy, popular items get most of the views, and each request carries its own
identifiers. diapason describes this with **scenarios** and **templates**,
compiled once at start-up: no script runs per request.

## A small shop

`users.csv`:

```text title="users.csv"
id,name,plan
1,alice,free
2,bob,pro
3,carol,free
4,dave,team
```

`shop.toml`:

```toml title="shop.toml"
seed = 7
threads = 2

[target]
url = "http://127.0.0.1:8080/"
headers = { "accept" = "application/json" }

[load]
[[load.phases]]
name = "warmup"
duration = "2s"
rate = 500
[[load.phases]]
name = "measure"
duration = "10s"
rate = 1000

[connections]
count = 20

[vars]
product = { dist = "zipf", n = 1000, s = 1.1 }
qty = { dist = "uniform", min = 1, max = 5 }
broken = { dist = "choice", values = [404, 500, 503], weights = [8, 1, 1] }

[data.users]
file = "users.csv"
mode = "random"

[[scenarios]]
name = "browse"
weight = 8
path = "/products/{{product}}"
headers = { "x-user" = "{{users.name}}" }

[[scenarios]]
name = "buy"
weight = 2
method = "POST"
path = "/cart"
headers = { "content-type" = "application/json" }
body = '{"user": {{users.id}}, "product": {{product}}, "qty": {{qty}}}'

[[scenarios]]
name = "errors"
weight = 0.5
path = "/status/{{broken}}"

[[thresholds]]
expr = "p99 < 50ms"

[[thresholds]]
expr = "error_rate < 1%"
```

- `browse` requests `/products/{{product}}` with a product number drawn from
  a Zipf distribution (a few products get most of the traffic) and a user
  name from a random row of `users.csv`.
- `buy` posts a JSON body; `Content-Length` is computed for each request.
- `errors` uses a path of the reference server, `/status/NNN`, that answers
  with status NNN, to show how status codes are counted.

```bash
diapason server --workers 8 --service exp:1ms &
diapason run --config shop.toml
```

```text
phase measure (measured) [2.0s, 12.0s): scheduled 9683 (968.3/s, nominal 1000.0/s), completed 9683, success 9205
  status: 200=9205 404=388 500=50 503=40
  ...
  scenario browse            76.07 % of requests (weight  76.19 %), completed 7366, success 7366, failed 0, not sent 0
  scenario buy               18.99 % of requests (weight  19.05 %), completed 1839, success 1839, failed 0, not sent 0
  scenario errors             4.94 % of requests (weight   4.76 %), completed 478, success 0, failed 0, not sent 0
fidelity: ok
  busiest worker: 2.0 % busy
thresholds:
  FAIL error_rate < 1% on measure: value 0.0494, limit 0.0100
exit 5: thresholds failed: error_rate < 1%
```

The report gives the realised mix next to the configured weights, and the
status codes one by one. The `errors` scenario makes 4.9 % of the requests
fail, so the `error_rate < 1%` threshold fails and the run exits with 5.

## Variables

| `dist` | parameters | value |
| --- | --- | --- |
| `uniform` | `min`, `max` | integer in [min, max] |
| `zipf` | `n`, `s` | integer k in [1, n], with probability proportional to 1/k^s |
| `normal` | `mean`, `stdev`, `round` | real number, or integer when `round = true` |
| `choice` | `values`, optional `weights` | one of the values |
| `sequence` | `start`, `step` | start + step × seq |

`{{seq}}` is the request's sequence number. A variable used twice in one
request has the same value both times.

## Data files

`[data.NAME]` reads a CSV file with a header line, or a JSON-lines file, and
`{{NAME.column}}` takes a column of the chosen row. `mode` picks the row:
`sequential` (row seq mod rows), `random`, or `unique` (row seq; requests
past the last row wrap around and are counted in the summary's
`protocol_report.data_wrapped`).

## Reproducible content

The scenario of a request, its variables and its random rows depend only on
the seed and the request's `seq`. With the same seed and configuration, a
run sends the same requests, whichever worker sends them and when, and even
when it is [split across several machines](distributed).
