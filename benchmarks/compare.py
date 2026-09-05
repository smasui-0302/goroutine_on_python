"""fresh processで反復測定。JSON Linesをstdoutへ、CPU比較のmedianをstderrへ。"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
from .common import positive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', choices=['cpu', 'tasks'], default='cpu')
    parser.add_argument('--repeats', type=positive, default=3)
    parser.add_argument('--tasks', type=positive, default=8)
    parser.add_argument('--iterations', type=positive, default=1_000_000)
    parser.add_argument('--workers', type=positive, nargs='+', default=[1, 2, 4])
    parser.add_argument('--green-tasks', type=positive, default=100_000)
    parser.add_argument('--thread-tasks', type=positive, default=100)
    args = parser.parse_args()
    if args.thread_tasks > 512:
        parser.error('比較runnerのOS thread上限は512です')
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ, PYTHONPATH=str(root / 'src'))
    if args.suite == 'cpu':
        common = ['--tasks', str(args.tasks), '--iterations', str(args.iterations)]
        cases = [('m1', ['benchmarks.cpu', '--mode', 'm1', *common])]
        cases += [(f'mn-{n}', ['benchmarks.cpu', '--mode', 'mn', '--workers', str(n), *common]) for n in args.workers]
    else:
        cases = [('threads', ['examples.os_threads', '--tasks', str(args.thread_tasks)]),
                 ('m1-small', ['examples.m1_many', '--tasks', str(args.thread_tasks)]),
                 ('asyncio-small', ['benchmarks.asyncio_baseline', '--tasks', str(args.thread_tasks)]),
                 ('m1-many', ['examples.m1_many', '--tasks', str(args.green_tasks)]),
                 ('asyncio-many', ['benchmarks.asyncio_baseline', '--tasks', str(args.green_tasks)])]
    samples = {name: [] for name, _ in cases}
    for repeat in range(args.repeats):
        # 実行順による温度/負荷の偏りを軽減（厳密な統計実験ではない）。
        ordered = cases if repeat % 2 == 0 else list(reversed(cases))
        for name, command in ordered:
            child = subprocess.run([sys.executable, '-m', *command], cwd=root, env=env,
                                   text=True, capture_output=True, timeout=360)
            if child.returncode:
                raise RuntimeError(f'{name} failed:\n{child.stdout}\n{child.stderr}')
            result = json.loads(child.stdout)
            samples[name].append(result['elapsed_s'])
            print(json.dumps(dict(result, case=name, repeat=repeat), ensure_ascii=False), flush=True)
    medians = {name: statistics.median(values) for name, values in samples.items()}
    summary = {'median_elapsed_s': medians}
    if args.suite == 'cpu':
        summary['speedup_vs_m1'] = {name: medians['m1'] / value for name, value in medians.items()}
    print(json.dumps(summary, ensure_ascii=False), file=sys.stderr)


if __name__ == '__main__':
    main()
