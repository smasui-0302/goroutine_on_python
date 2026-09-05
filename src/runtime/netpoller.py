"""Selectorは専用threadだけが操作する。Workerとの通信はQueue + socketpair。"""
import queue
import selectors
import socket
import threading
import time


class NetPoller:
    def __init__(self, ready, fatal):
        self._ready = ready
        self._fatal = fatal
        self._commands = queue.Queue()
        self._selector = selectors.DefaultSelector()
        try:
            self._reader, self._writer = socket.socketpair()
            self._reader.setblocking(False)
            self._writer.setblocking(False)
            self._selector.register(self._reader, selectors.EVENT_READ)
        except BaseException:
            self._selector.close()
            for sock in (getattr(self, '_reader', None), getattr(self, '_writer', None)):
                if sock is not None:
                    sock.close()
            raise
        self._thread = threading.Thread(target=self._run, name="netpoller")
        # fd -> (Task, WaitIO, deadline)。同じsocketの同時waitは明示的に拒否。
        self._waiting = {}

    def start(self):
        self._thread.start()

    def submit(self, task, request):
        deadline = None if request.timeout is None else time.monotonic() + request.timeout
        self._send((task, request, deadline))

    def _send(self, command):
        self._commands.put(command)
        try:
            self._writer.send(b'x')
        except BlockingIOError:
            pass  # wake byteが既にあるので通知は失われない

    def close(self):
        if self._thread.is_alive():
            self._send(None)
            self._thread.join()
        self._selector.close()
        self._reader.close()
        self._writer.close()
        self._waiting.clear()

    def _commands_ready(self):
        try:
            while self._reader.recv(4096):
                pass
        except BlockingIOError:
            pass
        while True:
            try:
                command = self._commands.get_nowait()
            except queue.Empty:
                return True
            if command is None:
                return False
            task, request, deadline = command
            try:
                if request.sock.getblocking():
                    raise ValueError("I/O socketにはsetblocking(False)が必要です")
                fd = request.sock.fileno()
                if fd in self._waiting:
                    raise ValueError("同じsocketで複数Taskを同時に待機させることはできません")
                self._selector.register(request.sock, request.events, fd)
                self._waiting[fd] = (task, request, deadline)
            except Exception as error:
                self._ready(task, error)

    def _release(self, fd, error=None):
        task, _, _ = self._waiting.pop(fd)
        self._selector.unregister(fd)
        self._ready(task, error)

    def _run(self):
        try:
            while True:
                # 教材として単純なO(waiters)のtimeout走査。大量timerにはheapが適する。
                now = time.monotonic()
                deadlines = [entry[2] for entry in self._waiting.values() if entry[2] is not None]
                timeout = max(0, min(deadlines) - now) if deadlines else None
                events = self._selector.select(timeout)
                # 古いready通知を処理してから新しい登録を受理する（fd再利用対策）。
                for key, _ in events:
                    if key.fileobj is not self._reader and key.data in self._waiting:
                        self._release(key.data)
                now = time.monotonic()
                for fd, (_, _, deadline) in list(self._waiting.items()):
                    if deadline is not None and deadline <= now:
                        self._release(fd, TimeoutError("socket readiness wait timed out"))
                if not self._commands_ready():
                    return
        except BaseException as error:
            self._fatal(error)
