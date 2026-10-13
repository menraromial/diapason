---
id: intro
title: Introduction
sidebar_label: Introduction
slug: /intro
---

diapason is a load generator for network services, in the family of wrk2,
Vegeta, hey and k6. It is meant for benchmarks, capacity planning, research
experiments and performance gates in CI, on any HTTP service.

What sets it apart is how it measures:

- requests leave on a schedule computed independently of the responses
  (open loop), so a slow server does not slow the load down;
- every scheduled request is accounted for: at the end of a run,
  `scheduled = completed + failed + not_sent + in_flight_at_end`, and a run
  where this does not hold exits with code 4;
- the client's own delays (generator lag, waiting for a connection) are
  recorded apart from the server's, and the latency that a user would have
  seen (corrected latency) is their sum;
- the run states whether the client kept to its schedule and whether its own
  limits shaped the load, through fidelity checks with their own exit code.

diapason does not judge the system under test. It writes exact,
machine-readable measurements (a JSON summary, HdrHistogram interval logs, a
per-request trace) for other tools to analyse.

## Why another load generator

Thousands of runs of wrk2 showed failure modes that its report does not
reveal:

- each of its HTTP/1.1 connections carries one request at a time, so once
  rate × latency exceeds the connection count the client becomes closed-loop
  without saying so;
- beyond the process file-descriptor limit, connections silently fail, and
  each dead connection then counts as a timeout every 2 s, whatever the rate;
- its requests per second are divided by a duration that includes a 10 s
  calibration phase;
- with many threads its arrivals become bursty: with 64 threads, 50 ms slices
  peaked at 62 times the mean rate and 97 % of the slices were empty;
- connections that do not divide evenly between threads are dropped (1000
  connections on 104 threads open 936).

Each of these has a counterpart in diapason: an admission model with its own
measurements and checks, refusal to start when the file-descriptor limit is
too low, timeouts counted once per request, rates computed on the
measurement phase only, an interleaved schedule (the same test at 64 threads
and 4032 connections gives a 50 ms peak/mean of 1.0 and no empty slice), and
remainders spread over workers. The validation tests check each one against
a reference server.

## Reading this documentation

- [Installation](installation) and [Quick start](quick-start) get a first
  run going.
- [Accounting model](accounting-model) defines every timestamp, delay,
  outcome and phase; the rest of the documentation relies on it.
- [Fidelity checks](fidelity-checks) explains how a run judges itself.
- [Configuration](configuration), [Command line](command-line),
  [Outputs](outputs) and [Library](library) are the reference.
- [Validation tests](validation) and
  [Design measurements](design-measurements) give the evidence behind the
  claims above.

The reference pages are generated from the repository's README, which is
the specification of the accounting model and of every output field.
