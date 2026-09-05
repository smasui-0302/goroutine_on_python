"""Taskがruntime実行中にchild / grandchildを生成する。"""
from runtime import Runtime, gosched, spawn


def main():
    runtime = Runtime()
    events = []

    def grandchild():
        events.append('grandchild')
        yield gosched()

    def child():
        events.append('child')
        grandchild_task = yield spawn(grandchild())
        assert grandchild_task.id > 0

    def parent():
        events.append('parent')
        child_task = yield spawn(child())
        assert child_task.id > 0

    runtime.go(parent())
    runtime.start_runtime(timeout=2)
    assert set(events) == {'parent', 'child', 'grandchild'}
    assert runtime.completed == 3 and runtime.spawned == 2
    print(events)


if __name__ == '__main__':
    main()
