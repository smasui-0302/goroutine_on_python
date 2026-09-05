"""比較用のみ。runtimeの実装からはimportしない。"""
import argparse
import asyncio
import time
from .common import Measurement, positive


async def work():
    await asyncio.sleep(0)
    return 1


async def run(count, measurement):
    tasks = [asyncio.create_task(work()) for _ in range(count)]
    spawn_s = time.perf_counter() - measurement.wall
    results = await asyncio.gather(*tasks)
    assert sum(results) == count
    return spawn_s


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tasks', type=positive, default=100_000)
    args = parser.parse_args()
    measurement = Measurement()
    spawn_s = asyncio.run(run(args.tasks, measurement))
    measurement.report(mode='asyncio-many', tasks=args.tasks, completed=args.tasks, spawn_s=spawn_s)


if __name__ == '__main__':
    main()
