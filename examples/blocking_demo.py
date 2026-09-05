"""通常のblocking呼び出しはgoschedとは異なりWorkerごと停止させる。"""
import argparse
import time
from runtime import Runtime, MNRuntime, gosched
from benchmarks.common import Measurement, positive


def work():
    time.sleep(0.05)
    yield gosched()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['m1', 'mn'], default='mn')
    parser.add_argument('--tasks', type=positive, default=32)
    parser.add_argument('--workers', type=positive, default=4)
    args = parser.parse_args()
    measurement = Measurement()
    runtime = Runtime() if args.mode == 'm1' else MNRuntime(args.workers)
    for _ in range(args.tasks):
        runtime.go(work())
    runtime.start_runtime(timeout=120)
    assert runtime.completed == args.tasks
    measurement.report(mode=f'blocking-{args.mode}', tasks=args.tasks, workers=runtime.workers,
                       completed=runtime.completed, sleep_per_task_s=0.05)


if __name__ == '__main__':
    main()
