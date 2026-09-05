"""cooperative sleep中にも1 Workerが別Taskを進められることを示す。"""
import time

from benchmarks.common import Measurement
from runtime import Runtime, gosched, sleep


def main():
    runtime = Runtime()
    events = []

    def sleeper():
        events.append('sleeper: start')
        yield sleep(0.1)
        events.append('sleeper: end')

    def runnable():
        events.append('runnable: start')
        yield gosched()
        events.append('runnable: end')

    measurement = Measurement()
    runtime.go(sleeper())
    runtime.go(runnable())
    runtime.start_runtime(timeout=2)
    assert events == ['sleeper: start', 'runnable: start', 'runnable: end', 'sleeper: end']
    print('\n'.join(events))
    measurement.report(mode='cooperative-sleep', completed=runtime.completed,
                       sleep_waits=runtime.sleep_waits)


if __name__ == '__main__':
    main()
