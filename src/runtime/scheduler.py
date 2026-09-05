"""M:1/M:N共通の状態機械。Task実行中にはqueueのlockを保持しない。"""
from collections import deque
import inspect
import threading
import time
from types import GeneratorType

from .netpoller import NetPoller
from .task import Sleep, Spawn, State, Task, WaitIO, YieldNow, validate_timeout
from .timer import TimerQueue


class TaskErrors(Exception):
    def __init__(self, failures):
        self.failures = failures
        super().__init__(f"{len(failures)} Task(s) failed: {failures[0].error!r}")


class Scheduler:
    _inline = True

    def __init__(self, workers=1, *, enable_io=False):
        if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
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
        self._timer = None
        self._deadline = None
        self.completed = 0
        self.failures = []
        self.cancelled = 0
        self.io_waits = 0
        self.sleep_waits = 0
        self.spawned = 0
        self.peak_waiting = 0
        self._waiting = 0

    def _validate_generator_locked(self, generator):
        if not isinstance(generator, GeneratorType):
            raise TypeError("go()/spawn()にはgenerator objectを渡してください")
        if inspect.getgeneratorstate(generator) != inspect.GEN_CREATED:
            raise ValueError("未開始generatorが必要です")
        if generator in self._generators:
            raise ValueError("同じgeneratorは重複登録できません")

    def _register_locked(self, generator, *, context=None, worker_id=None, spawned=False):
        self._validate_generator_locked(generator)
        task = Task(self._next_id, generator) if context is None else Task(self._next_id, generator, context)
        self._next_id += 1
        self._generators.add(generator)
        self._active[task.id] = task
        self._enqueue_ready_locked(task, worker_id=worker_id, new=True)
        if spawned:
            self.spawned += 1
        return task

    def go(self, generator):
        """start前にroot Taskを登録する。実行中の生成には `yield spawn(...)` を使う。"""
        with self._condition:
            if self._started:
                raise RuntimeError("runtime実行中はyield spawn(...)を使ってください")
            return self._register_locked(generator)

    def _enqueue_ready_locked(self, task, *, worker_id=None, new=False):
        self._queue.append(task)

    def _dequeue_ready_locked(self, worker_id):
        return self._queue.popleft() if self._queue else None

    def _clear_ready_locked(self):
        self._queue.clear()

    def _ready_count_locked(self):
        return len(self._queue)

    @property
    def pending(self):
        with self._condition:
            return len(self._active)

    @property
    def queue_size(self):
        with self._condition:
            return self._ready_count_locked()

    def _fatal_locked(self, error):
        if self._abort is None:
            self._abort = error
        self._condition.notify_all()

    def _fatal(self, error):
        with self._condition:
            self._fatal_locked(error)

    def _ready(self, task, error=None):
        with self._condition:
            if task.state is not State.WAITING or self._abort is not None:
                return
            self._waiting -= 1
            task.pending_error = error
            task.state = State.READY
            self._enqueue_ready_locked(task)
            self._condition.notify_all()

    def _finish_locked(self, task, state, value):
        task.state = state
        if state is State.DONE:
            task.result = value
            self.completed += 1
        else:
            task.error = value
            self.failures.append(task)
        del self._active[task.id]
        self._generators.remove(task.generator)

    def _next_task_locked(self, worker_id):
        while True:
            if self._deadline is not None and time.monotonic() >= self._deadline:
                self._fatal_locked(TimeoutError("runtime cooperative deadline exceeded"))
            if self._abort is not None or not self._active:
                return None
            task = self._dequeue_ready_locked(worker_id)
            if task is not None:
                return task
            remaining = None if self._deadline is None else max(0, self._deadline - time.monotonic())
            self._condition.wait(remaining)

    def _mark_running_locked(self, task, worker_id):
        assert task.state is State.READY
        task.state = State.RUNNING
        if task.last_worker is not None and task.last_worker != worker_id:
            task.migrations += 1
        task.last_worker = worker_id
        task.steps += 1

    def _mark_waiting_locked(self, task):
        task.state = State.WAITING
        self._waiting += 1
        self.peak_waiting = max(self.peak_waiting, self._waiting)

    def _ensure_timer_locked(self):
        if self._timer is None:
            timer = TimerQueue(self._ready, self._fatal)
            timer.start()
            self._timer = timer
        return self._timer

    def _handle_request_locked(self, task, request, worker_id):
        if isinstance(request, Spawn):
            try:
                child = self._register_locked(
                    request.generator,
                    context=task.context.copy(),
                    worker_id=worker_id,
                    spawned=True,
                )
            except Exception as error:
                task.pending_error = error
            else:
                task.pending_value = child
                task.has_pending_value = True
            task.state = State.READY
            self._enqueue_ready_locked(task, worker_id=worker_id)
        elif isinstance(request, Sleep):
            self._mark_waiting_locked(task)
            self.sleep_waits += 1
            self._ensure_timer_locked().submit(task, request.delay)
        elif isinstance(request, WaitIO) and self._poller is not None:
            self._mark_waiting_locked(task)
            self.io_waits += 1
            self._poller.submit(task, request)
        else:
            if not isinstance(request, YieldNow) and request is not None:
                task.pending_error = TypeError("unsupported yield; I/Oにはenable_io=Trueが必要です")
            task.state = State.READY
            self._enqueue_ready_locked(task, worker_id=worker_id)
        self._condition.notify_all()

    def _worker(self, worker_id):
        try:
            self._worker_loop(worker_id)
        except BaseException as error:
            self._fatal(error)

    def _worker_loop(self, worker_id):
        while True:
            with self._condition:
                task = self._next_task_locked(worker_id)
                if task is None:
                    return
                self._mark_running_locked(task, worker_id)
            try:
                request = task.advance()
            except StopIteration as done:
                with self._condition:
                    self._finish_locked(task, State.DONE, done.value)
                    self._condition.notify_all()
                continue
            except Exception as error:
                with self._condition:
                    self._finish_locked(task, State.FAILED, error)
                    self._condition.notify_all()
                continue
            except BaseException as error:
                self._fatal(error)
                return
            with self._condition:
                self._handle_request_locked(task, request, worker_id)

    def _close_remaining_tasks(self):
        for task in list(self._active.values()):
            try:
                task.context.run(task.generator.close)
            except BaseException as error:
                task.error = error
            task.state = State.CANCELLED
            self.cancelled += 1
        self._active.clear()
        self._clear_ready_locked()
        self._generators.clear()
        self._waiting = 0

    def start_runtime(self, *, timeout=None):
        """全Task終了まで待つ。timeoutはyield境界の協調的deadline。"""
        timeout = validate_timeout(timeout, allow_zero=False)
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
                try:
                    self._poller.close()
                except BaseException as error:
                    self._fatal(error)
                finally:
                    self._poller = None
            if self._timer is not None:
                try:
                    self._timer.close()
                except BaseException as error:
                    self._fatal(error)
                finally:
                    self._timer = None
            self._close_remaining_tasks()
        if self._abort is not None:
            raise self._abort
        if self.failures:
            raise TaskErrors(self.failures)
        return self.completed
