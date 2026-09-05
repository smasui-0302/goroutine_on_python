"""global queue + Worker別local queueを使う教材向けwork stealing runtime。"""
from collections import deque

from .scheduler import Scheduler


class WorkStealingRuntime(Scheduler):
    _inline = False

    def __init__(self, workers=1, *, enable_io=False):
        super().__init__(workers, enable_io=enable_io)
        self._local_queues = [deque() for _ in range(workers)]
        self._steal_cursor = [index + 1 for index in range(workers)]
        self.steals_attempted = 0
        self.steals_succeeded = 0
        self.local_queue_hits = 0
        self.global_queue_hits = 0

    def _enqueue_ready_locked(self, task, *, worker_id=None, new=False):
        if worker_id is None:
            self._queue.append(task)
        else:
            self._local_queues[worker_id].append(task)

    def _dequeue_ready_locked(self, worker_id):
        local = self._local_queues[worker_id]
        if local:
            self.local_queue_hits += 1
            return local.popleft()
        if self._queue:
            self.global_queue_hits += 1
            return self._queue.popleft()
        if self.workers == 1:
            return None
        start = self._steal_cursor[worker_id] % self.workers
        for offset in range(self.workers - 1):
            victim = (start + offset) % self.workers
            if victim == worker_id:
                continue
            self.steals_attempted += 1
            queue = self._local_queues[victim]
            if queue:
                self._steal_cursor[worker_id] = victim + 1
                self.steals_succeeded += 1
                return queue.pop()
        self._steal_cursor[worker_id] = start + 1
        return None

    def _clear_ready_locked(self):
        super()._clear_ready_locked()
        for queue in self._local_queues:
            queue.clear()

    def _ready_count_locked(self):
        return len(self._queue) + sum(map(len, self._local_queues))
