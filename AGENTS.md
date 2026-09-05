# Repository instructions

- 日本語で説明する。Python標準ライブラリ中心の学習用runtime。
- core (`src/runtime/`) にasyncio、greenlet等のschedulerを導入しない。
- generatorを実行する間、schedulerのConditionを保持しない。
- queue / lifecycle / active countは同じConditionで保護する。
- Selectorはnetpoller threadだけで操作する。
- 検証: `PYTHONPATH=src python3 -m unittest discover -s tests -v`
- 構文確認: `python3 -m compileall -q src examples benchmarks tests`
- demo: `PYTHONPATH=src python3 -m examples.m1_many`
- I/O: `PYTHONPATH=src python3 -m examples.netpoller_demo`
- benchmarkは並行実行せず、fresh processで測定する。
- OS threadの大量生成はデフォルト上限512を維持する。
