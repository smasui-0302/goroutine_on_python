"""Sleep requestをdeadline順に管理する小さなtimer queue。"""
import heapq
import itertools
import threading
import time


class TimerQueue:
    def __init__(self, ready, fatal):
        self._ready = ready
        self._fatal = fatal
        self._condition = threading.Condition()
        self._timers = []
        self._sequence = itertools.count()
        self._closing = False
        self._thread = threading.Thread(target=self._run, name="timer-queue")

    def start(self):
        self._thread.start()

    def submit(self, task, delay):
        deadline = time.monotonic() + delay
        with self._condition:
            heapq.heappush(self._timers, (deadline, next(self._sequence), task))
            self._condition.notify()

    def close(self):
        with self._condition:
            self._closing = True
            self._condition.notify()
        if self._thread.is_alive():
            self._thread.join()
        self._timers.clear()

    def _run(self):
        try:
            while True:
                with self._condition:
                    while not self._closing and not self._timers:
                        self._condition.wait()
                    if self._closing:
                        return
                    deadline, _, task = self._timers[0]
                    remaining = deadline - time.monotonic()
                    if remaining > 0:
                        self._condition.wait(remaining)
                        continue
                    heapq.heappop(self._timers)
                self._ready(task)
        except BaseException as error:
            self._fatal(error)
