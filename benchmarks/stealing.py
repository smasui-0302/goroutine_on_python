"""dynamic spawnで負荷をlocal queueへ偏らせ、work stealingを観測する。"""
import argparse
import threading

from runtime import MNRuntime, WorkStealingRuntime, gosched, spawn

from .common import Measurement, positive


def cpu_work(index, iterations, chunk, gate=None):
    if gate is not None and not gate.wait(timeout=30):
        raise TimeoutError("parent Taskが別Workerで再開されませんでした")
    total = 0
    for start in range(0, iterations, chunk):
        for number in range(start, min(start + chunk, iterations)):
            total += (number + index) * (number + index)
        yield gosched()
    return total


def expected_sum(index, iterations):
    sum_numbers = iterations * (iterations - 1) // 2
    sum_squares = (iterations - 1) * iterations * (2 * iterations - 1) // 6
    return sum_squares + 2 * index * sum_numbers + iterations * index * index


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("mn", "ws"), required=True)
    parser.add_argument("--workers", type=positive, default=4)
    parser.add_argument("--tasks", type=positive, default=24)
    parser.add_argument("--heavy-tasks", type=positive, default=6)
    parser.add_argument("--heavy-iterations", type=positive, default=1_000_000)
    parser.add_argument("--light-iterations", type=positive, default=100_000)
    parser.add_argument("--chunk", type=positive, default=25_000)
    args = parser.parse_args()
    if args.heavy_tasks > args.tasks:
        parser.error("--heavy-tasksは--tasks以下にしてください")

    runtime = MNRuntime(args.workers) if args.mode == "mn" else WorkStealingRuntime(args.workers)
    release_first = threading.Event()
    handles = []

    def parent():
        # 複数Worker時は最初のchildがowner Workerを待機させる。WSでは別Workerが
        # local queue末尾のparentをstealしない限り、以降のspawnへ進めない。
        gate = release_first if args.workers > 1 else None
        handles.append((yield spawn(cpu_work(0, args.heavy_iterations, args.chunk, gate))))
        for index in range(1, args.tasks):
            iterations = args.heavy_iterations if index < args.heavy_tasks else args.light_iterations
            handles.append((yield spawn(cpu_work(index, iterations, args.chunk))))
        release_first.set()

    measurement = Measurement()
    parent_task = runtime.go(parent())
    runtime.start_runtime(timeout=300)

    assert len(handles) == args.tasks
    assert runtime.completed == args.tasks + 1
    assert len({task.id for task in handles}) == args.tasks
    for index, task in enumerate(handles):
        iterations = args.heavy_iterations if index < args.heavy_tasks else args.light_iterations
        assert task.result == expected_sum(index, iterations)
    if args.mode == "ws" and args.workers > 1:
        assert runtime.steals_succeeded > 0

    measurement.report(
        mode=args.mode,
        scenario="dynamic-spawn-imbalance",
        workers=args.workers,
        tasks=args.tasks,
        completed=runtime.completed,
        heavy_tasks=args.heavy_tasks,
        heavy_iterations=args.heavy_iterations,
        light_iterations=args.light_iterations,
        chunk=args.chunk,
        migrations=parent_task.migrations + sum(task.migrations for task in handles),
        steals_attempted=getattr(runtime, "steals_attempted", 0),
        steals_succeeded=getattr(runtime, "steals_succeeded", 0),
        local_queue_hits=getattr(runtime, "local_queue_hits", 0),
        global_queue_hits=getattr(runtime, "global_queue_hits", 0),
    )


if __name__ == "__main__":
    main()
