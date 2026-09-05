import contextvars
import socket
import threading
import unittest
from unittest.mock import patch

from runtime import Runtime, MNRuntime, State, TaskErrors, gosched, wait_read
from runtime.io import recv, send_all


def once(value=42):
    yield gosched()
    return value


class SchedulingTests(unittest.TestCase):
    def test_round_robin_resume_return_and_removal(self):
        order = []
        def work(number):
            order.append((number, 'start'))
            yield gosched()
            order.append((number, 'end'))
            return number * 2
        runtime = Runtime()
        tasks = [runtime.go(work(n)) for n in range(3)]
        self.assertEqual(runtime.start_runtime(), 3)
        self.assertEqual(order, [(n, phase) for phase in ('start', 'end') for n in range(3)])
        self.assertEqual([task.result for task in tasks], [0, 2, 4])
        self.assertTrue(all(task.state is State.DONE and task.steps == 2 for task in tasks))
        self.assertEqual((runtime.pending, runtime.queue_size), (0, 0))
        self.assertEqual(len(runtime._generators), 0)

    def test_frame_preserves_local(self):
        def work():
            local_value = 123
            yield gosched()
            return local_value + 1
        gen = work()
        next(gen)
        self.assertEqual(gen.gi_frame.f_locals['local_value'], 123)
        with self.assertRaises(StopIteration) as done:
            next(gen)
        self.assertEqual(done.exception.value, 124)
        self.assertIsNone(gen.gi_frame)

    def test_100000_tasks(self):
        runtime = Runtime()
        for _ in range(100_000):
            runtime.go(once())
        self.assertEqual(runtime.start_runtime(timeout=30), 100_000)
        self.assertEqual((runtime.pending, runtime.queue_size), (0, 0))

    def test_mn_no_lost_or_duplicate_tasks(self):
        for workers in (1, 2, 4):
            with self.subTest(workers=workers):
                runtime = MNRuntime(workers)
                counts = [0] * 2000
                lock = threading.Lock()
                def work(index):
                    for _ in range(4):
                        with lock:
                            counts[index] += 1
                        yield gosched()
                    return index
                tasks = [runtime.go(work(i)) for i in range(len(counts))]
                self.assertEqual(runtime.start_runtime(timeout=15), len(counts))
                self.assertEqual(counts, [4] * len(counts))
                self.assertEqual([t.result for t in tasks], list(range(len(counts))))

    def test_workers_wait_when_queue_temporarily_empty(self):
        runtime = MNRuntime(4)
        barrier = threading.Barrier(4, timeout=3)
        def work():
            barrier.wait()
            yield gosched()
            return 1
        for _ in range(4):
            runtime.go(work())
        self.assertEqual(runtime.start_runtime(timeout=5), 4)

    def test_exception_does_not_lose_other_tasks(self):
        for runtime in (Runtime(), MNRuntime(3)):
            def bad():
                yield gosched()
                raise ValueError('intentional')
            failed = runtime.go(bad())
            good = runtime.go(once())
            with self.assertRaises(TaskErrors) as errors:
                runtime.start_runtime(timeout=3)
            self.assertEqual(errors.exception.failures, [failed])
            self.assertIs(failed.state, State.FAILED)
            self.assertIs(good.state, State.DONE)
            self.assertEqual(runtime.pending, 0)

    def test_unsupported_yield_is_thrown_into_generator(self):
        def work():
            with self.assertRaises(TypeError):
                yield 'unsupported'
            return 'caught'
        runtime = Runtime()
        task = runtime.go(work())
        runtime.start_runtime()
        self.assertEqual(task.result, 'caught')

    def test_contextvars_are_task_local(self):
        variable = contextvars.ContextVar('value', default=-1)
        for runtime in (Runtime(), MNRuntime(4)):
            def work(number):
                variable.set(number)
                for _ in range(10):
                    yield gosched()
                    self.assertEqual(variable.get(), number)
            for i in range(100):
                runtime.go(work(i))
            runtime.start_runtime(timeout=5)
            self.assertEqual(variable.get(), -1)

    def test_validation_and_one_shot(self):
        with self.assertRaises(ValueError):
            MNRuntime(0)
        runtime = Runtime()
        with self.assertRaises(TypeError):
            runtime.go(lambda: None)
        gen = once()
        runtime.go(gen)
        with self.assertRaises(ValueError):
            runtime.go(gen)
        runtime.start_runtime()
        with self.assertRaises(RuntimeError):
            runtime.start_runtime()
        new_gen = once()
        try:
            with self.assertRaises(RuntimeError):
                runtime.go(new_gen)
        finally:
            new_gen.close()

    def test_empty_runtime(self):
        for runtime in (Runtime(enable_io=True), MNRuntime(4, enable_io=True)):
            self.assertEqual(runtime.start_runtime(), 0)

    def test_deadline_closes_suspended_generators(self):
        for runtime in (Runtime(), MNRuntime(2)):
            closed = threading.Event()
            def forever():
                try:
                    while True:
                        yield gosched()
                finally:
                    closed.set()
            task = runtime.go(forever())
            with self.assertRaises(TimeoutError):
                runtime.start_runtime(timeout=0.03)
            self.assertTrue(closed.is_set())
            self.assertIs(task.state, State.CANCELLED)
            self.assertEqual(runtime.pending, 0)

    def test_thread_start_failure_cleans_up(self):
        runtime = MNRuntime(2, enable_io=True)
        task = runtime.go(once())
        with patch.object(threading.Thread, 'start', side_effect=RuntimeError('cannot start')):
            with self.assertRaisesRegex(RuntimeError, 'cannot start'):
                runtime.start_runtime()
        self.assertIs(task.state, State.CANCELLED)
        self.assertEqual(runtime.pending, 0)


    def test_partial_worker_start_failure_joins_started_threads(self):
        runtime = MNRuntime(3, enable_io=True)
        before = {t.ident for t in threading.enumerate()}
        def forever():
            while True:
                yield gosched()
        task = runtime.go(forever())
        original_start = threading.Thread.start
        def start(thread):
            if thread.name == 'worker-1':
                raise RuntimeError('worker start failed')
            original_start(thread)
        with patch.object(threading.Thread, 'start', start):
            with self.assertRaisesRegex(RuntimeError, 'worker start failed'):
                runtime.start_runtime(timeout=2)
        self.assertIs(task.state, State.CANCELLED)
        self.assertEqual({t.ident for t in threading.enumerate()}, before)


class IOTests(unittest.TestCase):
    def pair(self):
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        return a, b

    def test_wait_ready_and_other_task_progress(self):
        for runtime in (Runtime(enable_io=True), MNRuntime(2, enable_io=True)):
            a, b = self.pair()
            order = []
            lock = threading.Lock()
            def reader():
                with lock:
                    order.append('waiting')
                data = yield from recv(a, 1, timeout=2)
                with lock:
                    order.append('received')
                return data
            def sender():
                yield gosched()
                with lock:
                    order.append('send')
                yield from send_all(b, b'x', timeout=2)
            task = runtime.go(reader())
            runtime.go(sender())
            runtime.start_runtime(timeout=3)
            self.assertEqual(task.result, b'x')
            self.assertLess(order.index('send'), order.index('received'))
            self.assertEqual(runtime.pending, 0)

    def test_readiness_before_registration_is_not_lost(self):
        a, b = self.pair()
        b.send(b'x')
        def reader():
            yield wait_read(a, timeout=1)
            return a.recv(1)
        runtime = MNRuntime(2, enable_io=True)
        task = runtime.go(reader())
        runtime.start_runtime(timeout=2)
        self.assertEqual(task.result, b'x')

    def test_timeout_injected_and_catchable(self):
        for runtime in (Runtime(enable_io=True), MNRuntime(2, enable_io=True)):
            a, _ = self.pair()
            def reader():
                try:
                    yield from recv(a, 1, timeout=0.02)
                except TimeoutError:
                    return 'timed out'
            task = runtime.go(reader())
            runtime.start_runtime(timeout=2)
            self.assertEqual(task.result, 'timed out')

    def test_runtime_deadline_unblocks_waiting_workers(self):
        a, _ = self.pair()
        runtime = MNRuntime(3, enable_io=True)
        task = runtime.go(recv(a, 1))
        with self.assertRaises(TimeoutError):
            runtime.start_runtime(timeout=0.03)
        self.assertIs(task.state, State.CANCELLED)
        self.assertEqual(runtime.pending, 0)

    def test_registration_error_is_task_failure(self):
        a, _ = self.pair()
        a.close()
        def reader():
            yield wait_read(a, timeout=1)
        runtime = MNRuntime(2, enable_io=True)
        runtime.go(reader())
        with self.assertRaises(TaskErrors):
            runtime.start_runtime(timeout=2)

    def test_duplicate_wait_is_rejected_without_losing_first(self):
        a, _ = self.pair()
        runtime = MNRuntime(2, enable_io=True)
        def reader():
            yield wait_read(a, timeout=0.1)
        runtime.go(reader())
        runtime.go(reader())
        with self.assertRaises(TaskErrors) as error:
            runtime.start_runtime(timeout=2)
        self.assertEqual(len(error.exception.failures), 2)
        self.assertEqual({type(t.error) for t in error.exception.failures}, {ValueError, TimeoutError})

    def test_large_send_and_eof(self):
        a, b = self.pair()
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        payload = b'abcdef' * 100_000
        def sender():
            yield from send_all(a, payload, timeout=3)
            a.shutdown(socket.SHUT_WR)
        def reader():
            data = bytearray()
            while True:
                block = yield from recv(b, 8192, timeout=3)
                if not block:
                    return bytes(data)
                data.extend(block)
        runtime = MNRuntime(2, enable_io=True)
        runtime.go(sender())
        task = runtime.go(reader())
        runtime.start_runtime(timeout=5)
        self.assertEqual(task.result, payload)
        self.assertGreater(runtime.io_waits, 0)

    def test_many_waiters_with_one_worker(self):
        runtime = MNRuntime(1, enable_io=True)
        pairs = [self.pair() for _ in range(100)]
        def reader(sock):
            return (yield from recv(sock, 1, timeout=2))
        handles = [runtime.go(reader(a)) for a, _ in pairs]
        def sender():
            for _, b in pairs:
                yield from send_all(b, b'x', timeout=2)
        runtime.go(sender())
        runtime.start_runtime(timeout=3)
        self.assertTrue(all(t.result == b'x' for t in handles))
        self.assertEqual(runtime.peak_waiting, 100)

    def test_blocking_socket_rejected(self):
        a, _ = self.pair()
        a.setblocking(True)
        runtime = Runtime(enable_io=True)
        runtime.go(recv(a, 1))
        with self.assertRaises(TaskErrors) as error:
            runtime.start_runtime(timeout=1)
        self.assertIsInstance(error.exception.failures[0].error, ValueError)

    def test_poller_fatal_wakes_workers(self):
        a, _ = self.pair()
        runtime = MNRuntime(2, enable_io=True)
        runtime.go(recv(a, 1))
        with patch('selectors.DefaultSelector.select', side_effect=OSError('poll failed')):
            with self.assertRaisesRegex(OSError, 'poll failed'):
                runtime.start_runtime(timeout=2)
        self.assertEqual(runtime.pending, 0)

    def test_no_background_thread_leak(self):
        before = {t.ident for t in threading.enumerate()}
        for _ in range(5):
            a, b = self.pair()
            b.send(b'x')
            runtime = MNRuntime(3, enable_io=True)
            runtime.go(recv(a, 1, timeout=1))
            runtime.start_runtime(timeout=2)
        self.assertEqual({t.ident for t in threading.enumerate()}, before)


if __name__ == '__main__':
    unittest.main()
