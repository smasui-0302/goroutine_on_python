from .scheduler import Scheduler


class Runtime(Scheduler):
    """N Worker threadsで共有queueを処理。Worker affinityは保証しない。"""
    _inline = False
