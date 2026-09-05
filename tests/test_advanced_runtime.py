import math
import socket
import threading
import time
import unittest

from runtime import (
    MNRuntime,
    Runtime,
    State,
    TaskErrors,
    WorkStealingRuntime,
    gosched,
    sleep,
    spawn,
)
from runtime.io import recv, send_all


RUNTIMES = (
    lambda: Runtime(),
    lambda: MNRuntime(3),
    lambda: WorkStealingRuntime(3),
)


def one_step(value=None):
    yield gosched()
    return value


class BaseExceptionTests(unittest.TestCase):
    def test_exception_is_isolated_as_task_failure(self):
        def bad():
            yield gosched()
            raise ValueError("task failed")

        for make_runtime in RUNTIMES:
            with self.subTest(runtime=make_runtime):
                runtime = make_runtime()
                failed = runtime.go(bad())
                good = runtime.go(one_step("done"))
                with self.assertRaises(TaskErrors) as caught:
                    runtime.start_runtime(timeout=2)
                self.assertEqual(caught.exception.failures, [failed])
                self.assertIsInstance(failed.error, ValueError)
                self.assertIs(failed.state, State.FAILED)
                self.assertEqual(good.result, "done")

    def test_control_flow_base_exceptions_abort_and_cleanup(self):
        for exception in (KeyboardInterrupt("stop"), SystemExit(17)):
            for make_runtime in RUNTIMES:
                with self.subTest(exception=type(exception), runtime=make_runtime):
                    runtime = make_runtime()
                    closed = threading.Event()

                    def waiting():
                        try:
                            yield sleep(30)
                        finally:
                            closed.set()

                    def aborting():
                        yield gosched()
                        raise exception

                    before = {thread.ident for thread in threading.enumerate()}
                    waiting_task = runtime.go(waiting())
                    runtime.go(aborting())
                    with self.assertRaises(type(exception)) as caught:
                        runtime.start_runtime(timeout=2)
                    self.assertIs(caught.exception, exception)
                    self.assertNotIsInstance(caught.exception, TaskErrors)
                    self.assertTrue(closed.is_set())
                    self.assertIs(waiting_task.state, State.CANCELLED)
                    self.assertEqual(runtime.pending, 0)
                    self.assertEqual({thread.ident for thread in threading.enumerate()}, before)

    def test_abort_closes_netpoller_and_io_waiter(self):
        for runtime in (Runtime(enable_io=True), MNRuntime(2, enable_io=True),
                        WorkStealingRuntime(2, enable_io=True)):
            with self.subTest(runtime=runtime):
                left, right = socket.socketpair()
                left.setblocking(False)
                right.setblocking(False)
                closed = threading.Event()

                def io_waiter():
                    try:
                        yield from recv(left, 1)
                    finally:
                        closed.set()

                def aborting():
                    yield gosched()
                    raise KeyboardInterrupt("abort with poller")

                before = {thread.ident for thread in threading.enumerate()}
                runtime.go(io_waiter())
                runtime.go(aborting())
                try:
                    with self.assertRaises(KeyboardInterrupt):
                        runtime.start_runtime(timeout=2)
                finally:
                    left.close()
                    right.close()
                self.assertTrue(closed.is_set())
                self.assertEqual({thread.ident for thread in threading.enumerate()}, before)


class SleepTests(unittest.TestCase):
    def test_sleep_does_not_block_other_tasks(self):
        for make_runtime in RUNTIMES:
            with self.subTest(runtime=make_runtime):
                runtime = make_runtime()
                order = []

                def sleeper():
                    order.append("sleep-start")
                    yield sleep(0.03)
                    order.append("sleep-end")

                def runnable():
                    order.append("other")
                    yield gosched()

                runtime.go(sleeper())
                runtime.go(runnable())
                runtime.start_runtime(timeout=2)
                self.assertLess(order.index("other"), order.index("sleep-end"))
                self.assertEqual(runtime.sleep_waits, 1)

    def test_sleepers_resume_in_deadline_order(self):
        for make_runtime in RUNTIMES:
            with self.subTest(runtime=make_runtime):
                runtime = make_runtime()
                order = []

                def sleeper(name, delay):
                    yield sleep(delay)
                    order.append(name)

                runtime.go(sleeper("last", 0.05))
                runtime.go(sleeper("first", 0.01))
                runtime.go(sleeper("middle", 0.03))
                runtime.start_runtime(timeout=2)
                self.assertEqual(order, ["first", "middle", "last"])

    def test_sleep_validation(self):
        for invalid in (-1, math.nan, math.inf, -math.inf, True, "1"):
            with self.subTest(invalid=invalid):
                with self.assertRaises((TypeError, ValueError)):
                    sleep(invalid)


class SpawnTests(unittest.TestCase):
    def test_parent_child_grandchild_and_ids(self):
        for make_runtime in RUNTIMES:
            with self.subTest(runtime=make_runtime):
                runtime = make_runtime()
                events = []
                handles = []

                def grandchild():
                    events.append("grandchild")
                    yield gosched()

                def child():
                    events.append("child")
                    handles.append((yield spawn(grandchild())))

                def parent():
                    events.append("parent")
                    handles.append((yield spawn(child())))

                root = runtime.go(parent())
                runtime.start_runtime(timeout=2)
                self.assertEqual(set(events), {"parent", "child", "grandchild"})
                self.assertEqual(runtime.completed, 3)
                self.assertEqual(runtime.spawned, 2)
                self.assertEqual(len({root.id, *(task.id for task in handles)}), 3)
                self.assertTrue(all(task.state is State.DONE for task in handles))

    def test_many_spawned_tasks_are_included_in_completion(self):
        for make_runtime in RUNTIMES:
            with self.subTest(runtime=make_runtime):
                runtime = make_runtime()
                completed = []

                def child(number):
                    yield gosched()
                    completed.append(number)

                def parent():
                    for number in range(1000):
                        yield spawn(child(number))

                runtime.go(parent())
                runtime.start_runtime(timeout=10)
                self.assertEqual(sorted(completed), list(range(1000)))
                self.assertEqual(runtime.completed, 1001)
                self.assertEqual(runtime.spawned, 1000)
                self.assertEqual(runtime.pending, 0)

    def test_spawned_failure_does_not_deadlock(self):
        def bad_child():
            yield gosched()
            raise LookupError("child failed")

        for make_runtime in RUNTIMES:
            with self.subTest(runtime=make_runtime):
                runtime = make_runtime()

                def parent():
                    handle = yield spawn(bad_child())
                    return handle.id

                parent_task = runtime.go(parent())
                with self.assertRaises(TaskErrors) as caught:
                    runtime.start_runtime(timeout=2)
                self.assertEqual(len(caught.exception.failures), 1)
                self.assertIsInstance(caught.exception.failures[0].error, LookupError)
                self.assertIs(parent_task.state, State.DONE)
                self.assertEqual(runtime.pending, 0)


class WorkStealingTests(unittest.TestCase):
    def test_one_worker_completes_without_stealing(self):
        runtime = WorkStealingRuntime(1)
        tasks = [runtime.go(one_step(number)) for number in range(100)]
        runtime.start_runtime(timeout=2)
        self.assertEqual([task.result for task in tasks], list(range(100)))
        self.assertEqual(runtime.steals_attempted, 0)
        self.assertEqual(runtime.steals_succeeded, 0)
        self.assertGreater(runtime.global_queue_hits, 0)
        self.assertGreater(runtime.local_queue_hits, 0)

    def test_spawn_imbalance_causes_real_steal_without_duplicates(self):
        runtime = WorkStealingRuntime(4)
        counts = [0] * 2000
        lock = threading.Lock()
        release_first = threading.Event()

        def child(index, wait_for_parent=False):
            if wait_for_parent and not release_first.wait(timeout=5):
                raise TimeoutError("parent was not stolen")
            for _ in range(3):
                with lock:
                    counts[index] += 1
                yield gosched()

        def parent():
            # ownerはlocal queue先頭のchildで待機する。別Workerが末尾のparentを
            # stealして残りをspawnし、gateを開けるため、steal成功が必須になる。
            yield spawn(child(0, wait_for_parent=True))
            for index in range(1, len(counts)):
                yield spawn(child(index))
            release_first.set()

        runtime.go(parent())
        runtime.start_runtime(timeout=15)
        self.assertEqual(counts, [3] * len(counts))
        self.assertEqual(runtime.completed, len(counts) + 1)
        self.assertGreater(runtime.steals_attempted, 0)
        self.assertGreater(runtime.steals_succeeded, 0)
        self.assertGreater(runtime.local_queue_hits, 0)
        self.assertGreater(runtime.global_queue_hits, 0)


class TimeoutValidationTests(unittest.TestCase):
    def setUp(self):
        self.left, self.right = socket.socketpair()
        self.left.setblocking(False)
        self.right.setblocking(False)

    def tearDown(self):
        self.left.close()
        self.right.close()

    def test_recv_rejects_invalid_timeout_before_immediate_read(self):
        for invalid in (-1, math.nan, math.inf, -math.inf, True, "1"):
            with self.subTest(invalid=invalid):
                self.right.send(b"x")
                operation = recv(self.left, 1, timeout=invalid)
                with self.assertRaises((TypeError, ValueError)):
                    next(operation)
                self.left.recv(1)

    def test_send_rejects_invalid_timeout_before_immediate_write(self):
        for invalid in (-1, math.nan, math.inf, -math.inf, True, "1"):
            with self.subTest(invalid=invalid):
                operation = send_all(self.left, b"x", timeout=invalid)
                with self.assertRaises((TypeError, ValueError)):
                    next(operation)


if __name__ == "__main__":
    unittest.main()
