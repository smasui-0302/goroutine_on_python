"""generatorと、そのlifecycle。状態の変更はSchedulerのCondition内だけで行う。"""
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from enum import Enum, auto
from types import GeneratorType
from typing import Any
import math
import selectors
import socket


class State(Enum):
    READY = auto()
    RUNNING = auto()
    WAITING = auto()
    DONE = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass(frozen=True, slots=True)
class YieldNow:
    pass


_YIELD = YieldNow()


def gosched():
    """呼ぶだけでは切り替わらない。Task内で `yield gosched()` と書く。"""
    return _YIELD


@dataclass(frozen=True, slots=True)
class WaitIO:
    sock: socket.socket
    events: int
    timeout: float | None = None

    def __post_init__(self):
        if self.events not in (selectors.EVENT_READ, selectors.EVENT_WRITE):
            raise ValueError("readまたはwriteの一方向を指定してください")
        if self.timeout is not None and (not math.isfinite(self.timeout) or self.timeout < 0):
            raise ValueError("timeoutは有限の非負数です")


def wait_read(sock, timeout=None):
    return WaitIO(sock, selectors.EVENT_READ, timeout)


def wait_write(sock, timeout=None):
    return WaitIO(sock, selectors.EVENT_WRITE, timeout)


@dataclass(slots=True, eq=False)
class Task:
    id: int
    generator: GeneratorType
    context: Context = field(default_factory=copy_context, repr=False)
    state: State = State.READY
    result: Any = None
    error: BaseException | None = None
    pending_error: BaseException | None = field(default=None, repr=False)
    steps: int = 0
    last_worker: int | None = None
    migrations: int = 0

    def advance(self):
        # Schedulerが独占所有している間だけ呼ぶ。同時next()は絶対にしない。
        if self.pending_error is not None:
            error, self.pending_error = self.pending_error, None
            return self.context.run(self.generator.throw, error)
        return self.context.run(next, self.generator)
