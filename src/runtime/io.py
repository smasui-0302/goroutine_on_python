"""nonblocking socket用のgenerator helper。readinessは完了通知ではない。"""
import time
from .task import validate_timeout, wait_read, wait_write


def _remaining(deadline):
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("socket operation timed out")
    return remaining


def recv(sock, size, *, timeout=None):
    timeout = validate_timeout(timeout)
    if sock.getblocking():
        raise ValueError("setblocking(False)が必要です")
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        try:
            return sock.recv(size)  # b''はEOF
        except BlockingIOError:
            yield wait_read(sock, _remaining(deadline))


def send_all(sock, data, *, timeout=None):
    timeout = validate_timeout(timeout)
    if sock.getblocking():
        raise ValueError("setblocking(False)が必要です")
    deadline = None if timeout is None else time.monotonic() + timeout
    remaining = memoryview(data)
    while remaining:
        try:
            sent = sock.send(remaining)
            if sent == 0:
                raise ConnectionError("socket send returned zero")
            remaining = remaining[sent:]
        except BlockingIOError:
            yield wait_write(sock, _remaining(deadline))
