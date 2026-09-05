"""同じpure-Python計算をM:1/shared queue/work stealingで実行する。"""
import argparse
from runtime import Runtime, MNRuntime, WorkStealingRuntime, gosched
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
    parser.add_argument('--mode', choices=['m1', 'mn', 'ws'], default=default_mode or 'm1')
    parser.add_argument('--workers', type=positive, default=4)
    parser.add_argument('--tasks', type=positive, default=8)
    parser.add_argument('--iterations', type=positive, default=1_000_000)
    parser.add_argument('--chunk', type=positive, default=50_000)
    args = parser.parse_args()
    measurement = Measurement()
    runtimes = {
        'm1': lambda: Runtime(),
        'mn': lambda: MNRuntime(args.workers),
        'ws': lambda: WorkStealingRuntime(args.workers),
    }
    runtime = runtimes[args.mode]()
    tasks = [runtime.go(cpu_work(args.iterations, args.chunk)) for _ in range(args.tasks)]
    runtime.start_runtime(timeout=300)
    expected = (args.iterations - 1) * args.iterations * (2 * args.iterations - 1) // 6
    assert runtime.completed == args.tasks and all(task.result == expected for task in tasks)
    measurement.report(mode=args.mode, workers=runtime.workers, tasks=args.tasks, completed=runtime.completed,
                       iterations=args.iterations, chunk=args.chunk, checksum=sum(t.result for t in tasks),
                       migrations=sum(t.migrations for t in tasks),
                       steals_attempted=getattr(runtime, 'steals_attempted', 0),
                       steals_succeeded=getattr(runtime, 'steals_succeeded', 0),
                       local_queue_hits=getattr(runtime, 'local_queue_hits', 0),
                       global_queue_hits=getattr(runtime, 'global_queue_hits', 0))


if __name__ == '__main__':
    main()
