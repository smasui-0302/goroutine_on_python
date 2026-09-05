"""M:1/M:N共通の状態機械。Task実行中にはqueueのlockを保持しない。"""
from collections import deque
import inspect
import math
import threading
import time
from types import GeneratorType

from .netpoller import NetPoller
from .task import State, Task, WaitIO, YieldNow


class TaskErrors(Exception):
    def __init__(self, failures):
        self.failures = failures
        super().__init__(f"{len(failures)} Task(s) failed: {failures[0].error!r}")


class Scheduler:
    _inline = True

    def __init__(self, workers=1, *, enable_io=False):
        if not isinstance(workers, int) or workers < 1:
            raise ValueError("workersは1以上の整数です")
        self.workers = workers
        self.enable_io = enable_io
        self._condition = threading.Condition()
        self._queue = deque()
        self._active = {}
        self._generators = set()
        self._next_id = 0
        self._started = False
        self._abort = None
        self._poller = None
        self._deadline = None
        self.completed = 0
        self.failures = []
        self.cancelled = 0
        self.io_waits = 0
        self.peak_waiting = 0
        self._waiting = 0

    def go(self, generator):
        """未開始generatorを登録する。runtimeは一回限りのbatch。"""
        if not isinstance(generator, GeneratorType):
            raise TypeError("go()にはgenerator objectを渡してください")
        with self._condition:
            if self._started:
                raise RuntimeError("Task登録はstart_runtime()より前に行ってください")
            if inspect.getgeneratorstate(generator) != inspect.GEN_CREATED:
                raise ValueError("未開始generatorが必要です")
            # 登録重複の判定はO(1)。10万Taskの登録をO(M^2)にしない。
            if generator in self._generators:
                raise ValueError("同じgeneratorは重複登録できません")
            self._generators.add(generator)
            task = Task(self._next_id, generator)
            self._next_id += 1
            self._active[task.id] = task
            self._queue.append(task)
            return task

    @property
    def pending(self):
        with self._condition:
            return len(self._active)

    @property
    def queue_size(self):
        with self._condition:
            return len(self._queue)

    def _fatal(self, error):
        with self._condition:
            if self._abort is None:
                self._abort = error
            self._condition.notify_all()

    def _ready(self, task, error=None):
        with self._condition:
            if task.state is not State.WAITING or self._abort is not None:
                return
            self._waiting -= 1
            task.pending_error = error
            task.state = State.READY
            self._queue.append(task)
            self._condition.notify_all()

    def _finish(self, task, state, value):
        # Condition保持中に呼ぶ。
        task.state = state
        if state is State.DONE:
            task.result = value
            self.completed += 1
        else:
            task.error = value
            self.failures.append(task)
        del self._active[task.id]
        self._generators.remove(task.generator)

    def _worker(self, worker_id):
        try:
            self._worker_loop(worker_id)
        except BaseException as error:
            self._fatal(error)

    def _worker_loop(self, worker_id):
        while True:
            with self._condition:
                while True:
                    if self._deadline is not None and time.monotonic() >= self._deadline:
                        self._fatal(TimeoutError("runtime cooperative deadline exceeded"))
                    if self._abort is not None or not self._active:
                        return
                    if self._queue:
                        break
                    remaining = None if self._deadline is None else max(0, self._deadline - time.monotonic())
                    self._condition.wait(remaining)
                task = self._queue.popleft()
                assert task.state is State.READY
                task.state = State.RUNNING
                if task.last_worker is not None and task.last_worker != worker_id:
                    task.migrations += 1
                task.last_worker = worker_id
                task.steps += 1
            # このWorkerだけがtaskを所有。他Workerは同じgeneratorを実行できない。
            try:
                request = task.advance()
            except StopIteration as done:
                with self._condition:
                    self._finish(task, State.DONE, done.value)
                    self._condition.notify_all()
                continue
            except BaseException as error:
                with self._condition:
                    self._finish(task, State.FAILED, error)
                    self._condition.notify_all()
                continue
            with self._condition:
                if isinstance(request, WaitIO) and self._poller is not None:
                    task.state = State.WAITING
                    self._waiting += 1
                    self.io_waits += 1
                    self.peak_waiting = max(self.peak_waiting, self._waiting)
                    # WAITING設定→登録の順。即時readyでもlost wakeupしない。
                    self._poller.submit(task, request)
                else:
                    if not isinstance(request, YieldNow) and request is not None:
                        task.pending_error = TypeError("unsupported yield; I/Oにはenable_io=Trueが必要です")
                    task.state = State.READY
                    self._queue.append(task)
                self._condition.notify_all()

    def start_runtime(self, *, timeout=None):
        """全Task終了まで待つ。timeoutはyield境界の協調的deadline。"""
        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeoutは有限の正数です")
        with self._condition:
            if self._started:
                raise RuntimeError("runtimeは再実行できません")
            self._started = True
            self._deadline = None if timeout is None else time.monotonic() + timeout
        threads = []
        try:
            if self.enable_io and self.pending:
                self._poller = NetPoller(self._ready, self._fatal)
                self._poller.start()
            if self._inline:
                self._worker(0)
            else:
                for index in range(self.workers):
                    thread = threading.Thread(target=self._worker, args=(index,), name=f"worker-{index}")
                    thread.start()
                    threads.append(thread)
                for thread in threads:
                    thread.join()
        except BaseException as error:
            self._fatal(error)
        finally:
            for thread in threads:
                thread.join()
            if self._poller is not None:
                self._poller.close()
                self._poller = None
            # Worker/poller停止後のみgeneratorをcloseする。
            for task in list(self._active.values()):
                try:
                    task.context.run(task.generator.close)
                except BaseException as error:
                    task.error = error
                task.state = State.CANCELLED
                self.cancelled += 1
            self._active.clear()
            self._queue.clear()
            self._generators.clear()
            self._waiting = 0
        if self._abort is not None:
            raise self._abort
        if self.failures:
            raise TaskErrors(self.failures)
        return self.completed
