import argparse
import threading
import time
from benchmarks.common import Measurement, positive


def main():
    parser = argparse.ArgumentParser(description='1 task = 1 OS thread。全threadをgateで保持して生成コストを測る')
    parser.add_argument('--tasks', type=positive, default=100)
    parser.add_argument('--limit', type=positive, default=512, help='安全上限。大きな値にはOS資源枯渇の危険あり')
    args = parser.parse_args()
    if args.tasks > args.limit:
        parser.error('--tasksが--limitを超えています。まず小さな値で測定してください')
    gate = threading.Event()
    lock = threading.Lock()
    completed = 0

    def work():
        nonlocal completed
        gate.wait()
        with lock:
            completed += 1

    measurement = Measurement()
    threads = []
    failure = None
    try:
        for _ in range(args.tasks):
            thread = threading.Thread(target=work)
            try:
                thread.start()
            except RuntimeError as error:
                failure = str(error)
                break
            threads.append(thread)
    finally:
        spawn_s = time.perf_counter() - measurement.wall
        gate.set()
        for thread in threads:
            thread.join()
    measurement.report(mode='os-threads', tasks=args.tasks, started=len(threads), completed=completed,
                       spawn_s=spawn_s, creation_error=failure)
    if failure:
        raise SystemExit(1)
    assert completed == args.tasks


if __name__ == '__main__':
    main()
