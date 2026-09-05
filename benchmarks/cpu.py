"""同じpure-Python計算をM:1/M:Nで実行する。I/O/ログは計測中に行わない。"""
import argparse
from runtime import Runtime, MNRuntime, gosched
from .common import Measurement, positive


def cpu_work(iterations, chunk):
    total = 0
    for start in range(0, iterations, chunk):
        for number in range(start, min(start + chunk, iterations)):
            total += number * number
        yield gosched()
    return total


def main(default_mode=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['m1', 'mn'], default=default_mode or 'm1')
    parser.add_argument('--workers', type=positive, default=4)
    parser.add_argument('--tasks', type=positive, default=8)
    parser.add_argument('--iterations', type=positive, default=1_000_000)
    parser.add_argument('--chunk', type=positive, default=50_000)
    args = parser.parse_args()
    measurement = Measurement()
    runtime = Runtime() if args.mode == 'm1' else MNRuntime(args.workers)
    tasks = [runtime.go(cpu_work(args.iterations, args.chunk)) for _ in range(args.tasks)]
    runtime.start_runtime(timeout=300)
    expected = (args.iterations - 1) * args.iterations * (2 * args.iterations - 1) // 6
    assert runtime.completed == args.tasks and all(task.result == expected for task in tasks)
    measurement.report(mode=args.mode, workers=runtime.workers, tasks=args.tasks, completed=runtime.completed,
                       iterations=args.iterations, chunk=args.chunk, checksum=sum(t.result for t in tasks),
                       migrations=sum(t.migrations for t in tasks))


if __name__ == '__main__':
    main()
