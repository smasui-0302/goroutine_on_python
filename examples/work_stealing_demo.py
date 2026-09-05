"""dynamic spawnでlocal queueを偏らせ、idle Workerのstealを観測する。"""
import argparse
import threading

from benchmarks.common import Measurement, positive
from runtime import WorkStealingRuntime, gosched, spawn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers', type=positive, default=4)
    parser.add_argument('--tasks', type=positive, default=10_000)
    args = parser.parse_args()
    runtime = WorkStealingRuntime(args.workers)
    completed = 0
    lock = threading.Lock()
    release_first = threading.Event()

    def child(wait_for_parent=False):
        nonlocal completed
        if wait_for_parent and not release_first.wait(timeout=10):
            raise TimeoutError('parent was not stolen')
        for _ in range(3):
            yield gosched()
        with lock:
            completed += 1

    def parent():
        yield spawn(child(wait_for_parent=True))
        for _ in range(args.tasks - 1):
            yield spawn(child())
        release_first.set()

    measurement = Measurement()
    runtime.go(parent())
    runtime.start_runtime(timeout=120)
    assert completed == args.tasks
    measurement.report(
        mode='work-stealing-demo', workers=args.workers, tasks=args.tasks,
        completed=runtime.completed, spawned=runtime.spawned,
        steals_attempted=runtime.steals_attempted,
        steals_succeeded=runtime.steals_succeeded,
        local_queue_hits=runtime.local_queue_hits,
        global_queue_hits=runtime.global_queue_hits,
    )


if __name__ == '__main__':
    main()
