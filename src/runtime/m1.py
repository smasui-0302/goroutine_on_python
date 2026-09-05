from .scheduler import Scheduler


class Runtime(Scheduler):
    """呼び出し元の1 OS threadでround-robin実行。"""
    _inline = True

    def __init__(self, *, enable_io=False):
        super().__init__(1, enable_io=enable_io)
