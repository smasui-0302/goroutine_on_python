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


def validate_timeout(timeout, *, name="timeout", allow_zero=True):
    """公開APIで使うtimeoutの共通validation。Noneは無期限を表す。"""
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError(f"{name}は数値またはNoneです")
    value = float(timeout)
    minimum_ok = value >= 0 if allow_zero else value > 0
    if not math.isfinite(value) or not minimum_ok:
        qualifier = "非負" if allow_zero else "正"
        raise ValueError(f"{name}は有限の{qualifier}数です")
    return value


@dataclass(frozen=True, slots=True)
class Sleep:
    delay: float

    def __post_init__(self):
        object.__setattr__(self, "delay", validate_timeout(self.delay, name="delay"))


def sleep(delay):
    """Workerを止めず、このTaskだけを指定秒数WAITINGにする。"""
    return Sleep(delay)


@dataclass(frozen=True, slots=True)
class Spawn:
    generator: GeneratorType

    def __post_init__(self):
        if not isinstance(self.generator, GeneratorType):
            raise TypeError("spawn()にはgenerator objectを渡してください")


def spawn(generator):
    """runtime実行中にchild Taskを登録するscheduler request。"""
    return Spawn(generator)


@dataclass(frozen=True, slots=True)
class WaitIO:
    sock: socket.socket
    events: int
    timeout: float | None = None

    def __post_init__(self):
        if self.events not in (selectors.EVENT_READ, selectors.EVENT_WRITE):
            raise ValueError("readまたはwriteの一方向を指定してください")
        object.__setattr__(self, "timeout", validate_timeout(self.timeout))


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
    pending_value: Any = field(default=None, repr=False)
    has_pending_value: bool = field(default=False, repr=False)
    steps: int = 0
    last_worker: int | None = None
    migrations: int = 0

    def advance(self):
        # Schedulerが独占所有している間だけ呼ぶ。同時next()は絶対にしない。
        if self.pending_error is not None:
            error, self.pending_error = self.pending_error, None
            return self.context.run(self.generator.throw, error)
        if self.has_pending_value:
            value, self.pending_value = self.pending_value, None
            self.has_pending_value = False
            return self.context.run(self.generator.send, value)
        return self.context.run(next, self.generator)
