#!/usr/bin/env python3
"""Compares the latency quantiles of diapason, wrk2 and Vegeta on the
diapason reference server, below capacity, with the same rate and
connection count. Each tool runs on CPUs 0-5, the server on CPUs 12-21.

    scripts/compare.py --wrk2 PATH/wrk --vegeta PATH/vegeta [--runs 3]

The first block uses a server that answers at once, which measures each
tool's own floor; the second an M/G/16 queue with exponential service times
of mean 1 ms (capacity 16 000 req/s) at 2000 req/s.
"""
import argparse, json, os, re, subprocess, sys, tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIENT = ["taskset", "-c", "0-5"]
SERVER = ["taskset", "-c", "12-21"]
QS = [0.5, 0.9, 0.99, 0.999]


def diapason(a, url, i, tmp):
    out = os.path.join(tmp, f"d{i}.json")
    subprocess.run(CLIENT + [a.diapason, "run", "--url", url, "--rate", "2000", "--arrival", "constant",
                             "--warmup", "5s", "--duration", "20s", "--threads", "2", "--connections", "50",
                             "--seed", str(i), "--summary", out, "--quiet"])
    l = next(p for p in json.load(open(out))["phases"] if p["measured"])["latency"]["corrected"]
    return [l["p50_ms"], l["p90_ms"], l["p99_ms"], l["p99_9_ms"], l["max_ms"]]


def wrk2(a, url, i, tmp):
    # wrk2 spends its first 10 s calibrating; 30 s leaves 20 s at the rate.
    t = subprocess.run(CLIENT + [a.wrk2, "-t2", "-c50", "-d30s", "-R2000", "--latency", url],
                       capture_output=True, text=True).stdout
    unit = {"us": 1e-3, "ms": 1.0, "s": 1e3}
    q = {}
    for p, v, u in re.findall(r"^\s*(\d+\.\d+)%\s+([\d.]+)(us|ms|s)\s*$", t, re.M):
        q.setdefault(float(p), float(v) * unit[u])
    return [q[50.0], q[90.0], q[99.0], q[99.9], q[100.0]]


def vegeta(a, url, i, tmp):
    res = subprocess.run(CLIENT + [a.vegeta, "attack", "-rate=2000", "-duration=20s", "-connections=50"],
                         input=f"GET {url}\n".encode(), capture_output=True).stdout
    plot = subprocess.run([a.vegeta, "report", "-type=hdrplot"], input=res, capture_output=True).stdout.decode()
    rows = [l.split() for l in plot.splitlines() if l.strip() and l.split()[0][0].isdigit()]
    q = lambda p: next(float(r[0]) for r in rows if float(r[1]) >= p)
    return [q(p) for p in QS] + [float(rows[-1][0])]


def bench(a, name, server_args, tmp):
    srv = subprocess.Popen(SERVER + [a.diapason, "server", "--listen", "127.0.0.1:0", "--threads", "4"] + server_args,
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        line = srv.stdout.readline()
        assert line.startswith("listening "), line
        url = f"http://{line.split()[1]}/"
        print(f"== {name}: p50 p90 p99 p99.9 max (ms)")
        for i in range(1, a.runs + 1):
            for tool, f in [("diapason", diapason), ("wrk2", wrk2), ("vegeta", vegeta)]:
                print(f"{tool:<9}", " ".join(f"{x:7.3f}" for x in f(a, url, i, tmp)), flush=True)
    finally:
        srv.terminate()
        srv.wait()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--diapason", default=os.path.join(ROOT, "target/release/diapason"))
    p.add_argument("--wrk2", required=True)
    p.add_argument("--vegeta", required=True)
    p.add_argument("--runs", type=int, default=3)
    a = p.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        bench(a, "instant server (--workers 0)", ["--workers", "0"], tmp)
        bench(a, "M/G/16, exp 1 ms (capacity 16000/s), 2000 req/s", ["--workers", "16", "--service", "exp:1ms"], tmp)


if __name__ == "__main__":
    sys.exit(main())
