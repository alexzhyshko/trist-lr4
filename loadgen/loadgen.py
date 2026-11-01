"""Realistic-ish traffic generator for the demo app.

Arrivals are an open-loop Poisson process (clients don't wait for the server, like real
users) whose rate varies over time: slow daily-style wave + random bursts + jitter.

    python loadgen.py                       # 5 minutes, ~35 rps average
    python loadgen.py --duration 20 --rps 20
"""
import argparse
import asyncio
import math
import random
import statistics
import string
import time
from collections import Counter

import aiohttp

WORDS = ["order", "user", "login", "search", "cart", "invoice", "report", "item",
         "profile", "status", "export", "payment"]


def make_query() -> str:
    """Mostly short queries, some long, a few empty -- like real users."""
    r = random.random()
    if r < 0.10:
        return ""
    if r < 0.80:
        return random.choice(WORDS)
    if r < 0.97:
        return " ".join(random.choices(WORDS, k=random.randint(2, 6)))
    return "".join(random.choices(string.ascii_lowercase + " ", k=random.randint(100, 400)))


class RateModel:
    """Target requests/sec at time t."""

    def __init__(self, base_rps: float, duration: float):
        self.base, self.duration = base_rps, duration
        self.bursts = []  # (start, end, multiplier)
        t = random.uniform(15, 40)
        while t < duration:
            length = random.uniform(5, 15)
            self.bursts.append((t, t + length, random.uniform(1.8, 2.8)))
            t += length + random.uniform(30, 70)

    def rate(self, t: float) -> float:
        wave = 1 + 0.45 * math.sin(2 * math.pi * t / self.duration * 2 - math.pi / 2)
        mult = 1.0
        for s, e, m in self.bursts:
            if s <= t < e:
                mult = m
        jitter = random.uniform(0.9, 1.1)
        return max(1.0, self.base * wave * mult * jitter)


async def one_request(session, url, stats, latencies):
    start = time.perf_counter()
    try:
        async with session.get(url, params={"query": make_query()}) as resp:
            await resp.read()
            stats[str(resp.status)] += 1
    except asyncio.TimeoutError:
        stats["timeout"] += 1
    except aiohttp.ClientError:
        stats["conn_error"] += 1
    else:
        latencies.append(time.perf_counter() - start)


async def main(args):
    url = f"{args.url.rstrip('/')}/log"
    model = RateModel(args.rps, args.duration)
    stats, latencies, tasks = Counter(), [], set()
    conn = aiohttp.TCPConnector(limit=0)
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    t0 = time.perf_counter()
    sent, next_report = 0, 10

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        while (now := time.perf_counter() - t0) < args.duration:
            task = asyncio.create_task(one_request(session, url, stats, latencies))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            sent += 1
            if now >= next_report:
                print(f"[{now:5.0f}s] target {model.rate(now):5.1f} rps | sent {sent} | "
                      f"in flight {len(tasks)} | {dict(stats)}", flush=True)
                next_report += 10
            await asyncio.sleep(random.expovariate(model.rate(now)))  # Poisson arrivals
        if tasks:
            await asyncio.wait(tasks, timeout=args.timeout + 1)

    elapsed = time.perf_counter() - t0
    print(f"\nDone: {sent} requests in {elapsed:.0f}s ({sent / elapsed:.1f} rps avg)")
    print("Status codes:", dict(stats))
    if len(latencies) >= 20:
        q = statistics.quantiles(latencies, n=100)
        print(f"Client-side latency: p50={q[49] * 1000:.0f}ms p95={q[94] * 1000:.0f}ms "
              f"p99={q[98] * 1000:.0f}ms max={max(latencies) * 1000:.0f}ms")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:5000")
    ap.add_argument("--duration", type=float, default=300, help="seconds (default 300)")
    ap.add_argument("--rps", type=float, default=35, help="average requests/sec")
    ap.add_argument("--timeout", type=float, default=10, help="client timeout, seconds")
    asyncio.run(main(ap.parse_args()))
