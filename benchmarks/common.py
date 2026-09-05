import argparse
import json
import os
import platform
import sys
import sysconfig
import time


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('1以上を指定してください')
    return number


def environment():
    check = getattr(sys, '_is_gil_enabled', None)
    return {
        'python': sys.version.splitlines()[0],
        'executable': sys.executable,
        'platform': platform.platform(),
        'cpu_count': os.cpu_count(),
        'free_threaded_build': sysconfig.get_config_var('Py_GIL_DISABLED') == 1,
        'gil_enabled': check() if check else (True if sys.implementation.name == 'cpython' else None),
    }


def peak_rss_bytes():
    # ru_maxrssはprocess生涯のpeak。仮想メモリ/stack予約量ではない。
    try:
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == 'darwin':
            return int(value)
        if sys.platform.startswith('linux'):
            return int(value * 1024)
    except ImportError:
        pass
    return None


class Measurement:
    def __init__(self):
        self.wall = time.perf_counter()
        self.cpu = time.process_time()
        self.rss = peak_rss_bytes()

    def report(self, **fields):
        wall = time.perf_counter() - self.wall
        cpu = time.process_time() - self.cpu
        result = {**environment(), **fields, 'elapsed_s': wall, 'process_cpu_s': cpu,
                  'cpu_to_wall': cpu / wall if wall else None,
                  'peak_rss_bytes': peak_rss_bytes(), 'baseline_peak_rss_bytes': self.rss}
        print(json.dumps(result, ensure_ascii=False))
        return result
