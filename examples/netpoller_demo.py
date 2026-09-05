"""多数socketの待機と少数Worker。1つのproducer threadが外部の送信者を模擬する。"""
import argparse
import socket
import threading
import time
from runtime import Runtime, MNRuntime
from runtime.io import recv
from benchmarks.common import Measurement, positive


def run_demo(count=100, workers=2, mode='mn', delay=0.05):
    runtime = Runtime(enable_io=True) if mode == 'm1' else MNRuntime(workers, enable_io=True)
    pairs = []
    handles = []
    abort = threading.Event()
    condition = threading.Condition()
    entered = 0
    producer_errors = []

    def reader(sock):
        nonlocal entered
        with condition:
            entered += 1
            condition.notify_all()
        return (yield from recv(sock, 1, timeout=5))

    def producer():
        try:
            with condition:
                if not condition.wait_for(lambda: entered == count or abort.is_set(), timeout=5):
                    raise TimeoutError('readers did not start')
            if abort.wait(delay):
                return
            for _, writer in pairs:
                writer.sendall(b'x')
        except BaseException as error:
            producer_errors.append(error)

    measurement = Measurement()
    thread = None
    try:
        for _ in range(count):
            pair = socket.socketpair()
            pairs.append(pair)
            pair[0].setblocking(False)
            pair[1].settimeout(5)
            handles.append(runtime.go(reader(pair[0])))
        thread = threading.Thread(target=producer, name='external-producer')
        thread.start()
        runtime.start_runtime(timeout=10)
        thread.join()
        if producer_errors:
            raise producer_errors[0]
        assert runtime.completed == count and all(task.result == b'x' for task in handles)
        assert runtime.io_waits > 0
        return measurement.report(mode=f'netpoller-{mode}', tasks=count, workers=runtime.workers,
                                  completed=runtime.completed, io_waits=runtime.io_waits,
                                  peak_waiting=runtime.peak_waiting, producer_threads=1, poller_threads=1)
    finally:
        abort.set()
        with condition:
            condition.notify_all()
        if thread is not None and thread.ident is not None:
            thread.join()
        for pair in pairs:
            for sock in pair:
                sock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sockets', type=positive, default=100)
    parser.add_argument('--workers', type=positive, default=2)
    parser.add_argument('--mode', choices=['m1', 'mn'], default='mn')
    args = parser.parse_args()
    run_demo(args.sockets, args.workers, args.mode)


if __name__ == '__main__':
    main()
