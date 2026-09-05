from runtime import Runtime, gosched


def work(number):
    print(f'Task {number}: start')
    yield gosched()
    print(f'Task {number}: end')


def main():
    runtime = Runtime()
    for number in range(1, 4):
        runtime.go(work(number))
    runtime.start_runtime()
    assert runtime.completed == 3 and runtime.queue_size == 0


if __name__ == '__main__':
    main()
