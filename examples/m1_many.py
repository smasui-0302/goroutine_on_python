import argparse
import time
from runtime import Runtime, gosched
from benchmarks.common import Measurement, positive


def work():
    yield gosched()
    return 1


def main():
    parser = argparse.ArgumentParser(description='全Taskを登録してからround-robin実行')
    parser.add_argument('--tasks', type=positive, default=100_000)
    args = parser.parse_args()
    measurement = Measurement()
    runtime = Runtime()
    for _ in range(args.tasks):
        runtime.go(work())
    spawn_s = time.perf_counter() - measurement.wall
    runtime.start_runtime(timeout=120)
    assert runtime.completed == args.tasks and runtime.pending == 0
    measurement.report(mode='m1-many', tasks=args.tasks, completed=runtime.completed, spawn_s=spawn_s)


if __name__ == '__main__':
    main()
