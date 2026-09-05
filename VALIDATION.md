# 実行検証記録

2026-09-05に作業環境で実行。以下はこの環境の観測値であり、性能の保証値ではありません。READMEには再実行手順と結果の読み方を記載しています。

- Python: `3.13.5 (main, Jun 11 2025, 15:36:57) [Clang 17.0.0 (clang-1700.0.13.3)]`
- OS: `macOS-26.3.1-arm64-arm-64bit-Mach-O`
- logical CPU数: 10
- GIL有効。free-threaded buildは未検証。

## Tests / demo

- `PYTHONPATH=src python3 -m unittest discover -s tests -v`: pass。
- 最終構成の24 testsは `sys.setswitchinterval(0.00001)` を設定したunittest discoveryでもすべてpass。
- `python3 -m compileall -q src examples benchmarks tests`: pass。
- `examples.m1_demo`: 指定のstart 1/2/3 → end 1/2/3順で完了。
- `examples.m1_many`: 100,000 Taskを完了。
- `examples.os_threads --tasks 100`: 起動/完了とも100。
- `examples.m1_cpu_bound` / `examples.mn_cpu_bound`: 二乗和の期待値照合を含め完了。
- `examples.netpoller_demo --mode m1` / `--mode mn`: 100 socketをそれぞれ1 / 2 Workerで処理。両方ともpeak_waiting=100、completed=100。
- `examples.blocking_demo --mode m1` / `--mode mn`: 32 Taskをそれぞれ1 / 4 Workerで完了。
- `benchmarks.asyncio_baseline`: 100,000 Taskを完了。
- `benchmarks.compare`: cpu / tasksの両suiteを各3反復、fresh processで完了。最終測定はsuite同士を並行実行せず順番に実行。

## CPU測定

コマンド: `PYTHONPATH=src python3 -m benchmarks.compare --suite cpu --iterations 5000000 --repeats 3`

8 Task、各5,000,000 iteration、chunk=50,000。各sampleのchecksumを検証済み。

| 方式 | elapsed median (s) | 対M:1速度比 | CPU / wall median | migrations（3 sample） |
| --- | ---: | ---: | ---: | --- |
| m1 | 1.0974 | 1.000 | 0.9990 | 0, 0, 0 |
| mn-1 | 1.0929 | 1.004 | 0.9992 | 0, 0, 0 |
| mn-2 | 1.0753 | 1.021 | 1.0000 | 558, 562, 561 |
| mn-4 | 1.0971 | 1.000 | 1.0008 | 641, 649, 687 |

Worker移動は発生していますが、CPU / wallは約1、4 Workerの速度比も約1です。GIL有効時のpure-Python CPU並列性の制約と整合します。わずかな速度差だけを並列化の証拠とは扱いません。

## Task数の測定

コマンド: `PYTHONPATH=src python3 -m benchmarks.compare --suite tasks --repeats 3`

| 方式 | Task数 | elapsed median (s) | peak RSS median (MiB) |
| --- | ---: | ---: | ---: |
| threads | 100 | 0.003507 | 19.25 |
| m1-small | 100 | 0.000534 | 20.84 |
| asyncio-small | 100 | 0.001020 | 24.28 |
| m1-many | 100000 | 0.230511 | 90.52 |
| asyncio-many | 100000 | 0.343364 | 156.64 |

RSSはprocess全体の生涯peakです。Task数・待機方法・handle保持の違いに注意してください。OS threadの最大数を探索した結果ではありません。

詳細sampleは作業ディレクトリの `benchmark-results-cpu.jsonl` / `benchmark-results-tasks.jsonl`、medianは各 `-summary.jsonl` に保存しています。再生成可能な環境依存データとしてgit対象外です。

## 未確認・制約

- Linux / Windows / Python 3.11, 3.12、通常CPython 3.14等では未実行。
- Rust / Go本体は実行していません。比較表は参考記事・公式資料に基づく構造上の比較です。
- 外部lint / type-checkは設定・実行していません。標準ライブラリのtestと構文checkを実行しました。
- cooperativeな制約により、yieldしない無限loopやblocking関数をtimeoutで強制停止できません。
- OS threadを10万個生成する危険な限界試験は実施していません。

## free-threaded CPythonの追加検証

- `brew install uv` でuv 0.12.10を導入。
- `uv python install 3.14t` でuv管理のCPython 3.14.7 free-threaded buildを導入。
- `.python-version` を `3.14+freethreaded` にpinし、`.venv`をこのinterpreterで作成。
- `Py_GIL_DISABLED=1`、`sys._is_gil_enabled()=False` を確認。
- `PYTHONPATH=src PYTHON_GIL=0 .venv/bin/python -m unittest discover -s tests -v`: 24 testsがpass。
- `PYTHONPATH=src PYTHON_GIL=0 .venv/bin/python -m benchmarks.compare --suite cpu --iterations 5000000 --workers 1 2 4 --repeats 3`: M:1=0.8278秒、M:N 2=0.4232秒（1.956倍）、M:N 4=0.2412秒（3.432倍）。

## Phase 8〜10の追加検証

- `Exception`と`BaseException`を分離し、ValueErrorの隔離、KeyboardInterrupt/SystemExitの伝播、残存generator・Worker・poller・timerのcleanupをtest。
- `WorkStealingRuntime`を1 Workerと4 Workerでtestし、dynamic spawnで意図的にlocal queueを偏らせたscenarioで実stealを確認。
- `examples.work_stealing_demo --workers 4 --tasks 10000`: completed=10001、spawned=10000、steals_succeededを観測。
- `examples.sleep_demo`: 1 Workerでsleep中にrunnable Taskが完了し、deadline後にsleeperが復帰。
- `examples.spawn_demo`: parent → child → grandchildをruntime中に登録して3 Task完了。
- CPU比較へwork stealingを追加。JSONでelapsed、CPU / wall、migrations、steal/local/global queue統計を出力。
- free-threaded・GIL無効で2反復したCPU比較のmedianは、M:1=0.1644秒、M:N 2 Worker=0.0847秒、M:N 4 Worker=0.0480秒、work stealing 2 Worker=0.0888秒、work stealing 4 Worker=0.0539秒。短い測定なので傾向確認用。
- 通常CPython 3.13.5で37 testsがpass。
- CPython 3.14.7 free-threaded buildのGIL無効・有効の両設定で37 testsを実行。
- `.github/workflows/test.yml`を追加。通常CPython 3.11〜3.14と3.14t（GIL無効/有効）でunittestとcompileallを実行する構成。
- NetPollerのread/write同時waitと、I/O timeoutの共通timer heap化は実装していない。責務と世代/cancel管理が複雑になるため、既存のfdごと1 waiterと線形timeout走査を維持し、READMEに制約を明記。
