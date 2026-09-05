# Pythonで作る小さなgoroutine風runtime

PythonのgeneratorをTaskとして、自作のrun queueで動かす学習用プロジェクトです。M:1、共有queueを使ったM:N、selectorsによるI/O待ちを実装しています。runtime・testともに標準ライブラリだけで動きます。コードはPython 3.11以上を対象とし、このREADMEではuv管理のCPython 3.14 free-threaded版を使います。

出発点は[「goroutineを作ってみる。Rustで」](https://www.m3tech.blog/entry/build-your-own-goroutine-in-rust)です。元記事の専用スタック・レジスタ切り替えを移植するのではなく、generatorの停止・再開に置き換え、スケジューリングとI/O待ちを実験できるようにしました。元記事のRust実装にはnetpollerは含まれず、後半でその必要性が説明されています。

READMEは、OS Thread → M:1 → 100,000 Tasks → M:N shared queue → GIL → free-threaded Python → netpoller → local queue + work stealing → timer queue → dynamic spawn → Go runtimeとの差、というPhase順で読み進められます。timerを先に理解したい場合は、Phase 7の次にPhase 9を読み、Phase 8へ戻ると、netpoller → timer queue → local queueの順でも追えます。

## 最初に実行する

macOSでHomebrewがある場合は、まずuvを導入します。導入済みならこのコマンドは不要です。他OSの導入方法は[uv公式installation guide](https://docs.astral.sh/uv/getting-started/installation/)を参照してください。

```sh
brew install uv
```

続いてrepoのルートで実行します。以下のコマンドはすべてこのディレクトリから実行してください。

```sh
uv python install 3.14t
uv sync
export PYTHONPATH="$PWD/src"
uv run python -VV
uv run python -c 'import sys; print("GIL enabled:", sys._is_gil_enabled())'

uv run python -m examples.m1_demo
uv run python -m examples.m1_many --tasks 100000
uv run python -m examples.netpoller_demo --sockets 100 --workers 2
uv run python -m unittest discover -s tests -v
```

`.python-version` に `3.14+freethreaded` を保存しています。`uv sync` はそのPythonで`.venv`を作成し、このプロジェクトをeditable installします。`uv run` はその環境を選ぶため、activateは不要です。`uv.lock` はプロジェクトの解決結果を記録します。初回はPython本体とbuild用のsetuptoolsの取得にネットワークが必要です。

`PYTHONPATH` はこのshellで `src/runtime` を確実に読み込むための設定です。新しいterminalではrepoのルートで再設定してください。この検証環境ではmacOSのhidden属性が付いた`.pth`がPythonに読み飛ばされるため、editable installだけに依存せずsource pathを明示します。

`-VV` に `free-threading build`、GILの確認に `False` が出れば準備完了です。`3.14`だけの指定では通常版が優先されるため、free-threaded版の導入には`3.14t`を指定します。選択の詳細は[uvのPython versions](https://docs.astral.sh/uv/concepts/python-versions/#free-threaded-python)を参照してください。

このREADMEの環境変数指定はmacOS/Linuxのshell用です。PowerShellでは `export PYTHONPATH=...` を `$env:PYTHONPATH = "$PWD/src"` に置き換えてください。GILの比較では先に `$env:PYTHON_GIL = "0"` または `"1"` を設定し、その後に `uv run ...` を実行します。比較後は `Remove-Item Env:PYTHON_GIL` で解除できます。

最初のdemoは以下の順序になります。

```text
Task 1: start
Task 2: start
Task 3: start
Task 1: end
Task 2: end
Task 3: end
```

## 構成と読む順序

| ファイル | 責務 |
| --- | --- |
| `src/runtime/task.py` | generator / Python context / lifecycle / yield要求 |
| `src/runtime/scheduler.py` | queue、Task所有権、完了判定、例外・終了処理 |
| `src/runtime/m1.py` | 呼び出し元threadで実行するM:1 |
| `src/runtime/mn.py` | N個のWorkerで実行するM:N |
| `src/runtime/workstealing.py` | Worker別local queueとwork stealing |
| `src/runtime/netpoller.py` | Selectorの登録、ready通知、I/O timeout |
| `src/runtime/timer.py` | cooperative sleepのdeadline min-heap |
| `src/runtime/io.py` | nonblocking recv / partial sendを扱うhelper |
| `examples/` | 各phaseの実行可能なdemo |
| `benchmarks/` | 計測、fresh processでの比較、asyncio baseline |
| `tests/test_runtime.py` | Phase 1〜7の正常動作・競合・I/O・失敗時の終了のtest |
| `tests/test_advanced_runtime.py` | BaseException、work stealing、timer、spawn、timeoutのtest |

まず `examples/m1_demo.py`、`task.py`、`scheduler.py` の `_worker_loop()` を読むと、Taskを取り出し、1回再開し、queueへ戻す流れを追えます。

## Phase 1: 1 task = 1 OS thread

```sh
uv run python -m examples.os_threads --tasks 100
uv run python -m examples.os_threads --tasks 300
```

各threadをEventのgateで待機させ、全threadの生成後に一斉に解放します。単に短い関数を大量に起動すると、生成途中に既存threadが終了し、同時に存在するthread数のコストが見えなくなるためです。`started` と `completed` を確認してください。

OS threadにはカーネルが管理する実行状態、native stack、生成・同期・切り替えのコストがあります。メモリでは**stackの仮想アドレス予約量と、実際に触れたページのRSSは別**です。「thread数 × stack上限」がそのまま実RAM消費になるわけではありません。小規模・短時間の実験だけでOS threadが常に遅いとは結論できません。

デフォルトの安全上限は512です。`--tasks` が超えると生成前に拒否します。上限自体は `--limit` で変更できますが、OS threadで10万個を試す必要はありません。生成失敗は `creation_error` に出力し、起動済みthreadを解放・joinして非zeroで終了します。OS全体の資源枯渇を完全には防げないため、限界の自動探索は行いません。

## Phase 2: M:1とcooperativeな切り替え

```python
from runtime import Runtime, gosched

runtime = Runtime()

def work(number):
    print(number, "start")
    yield gosched()
    print(number, "end")
    return number

handles = [runtime.go(work(n)) for n in range(3)]
runtime.start_runtime()
assert [task.result for task in handles] == [0, 1, 2]
```

`go()` と `start_runtime()` はRuntimeのinstance methodです。globalな暗黙のruntimeはありません。`go()` は未開始のgenerator objectを受け取り、Task handleを返します。同じgeneratorの重複登録は拒否します。runtime間で同じgeneratorを共有したり、外部から `next()` / `close()` したりしないでください。

`deque.popleft()` → `next(generator)` → `yield`なら末尾へ戻す、`StopIteration`なら終了、というround-robinです。`yield None` も実行権を譲ります。`gosched()` を**呼ぶだけでは切り替わりません**。必ず `yield gosched()` と書きます。

Task切り替えごとにOS threadを切り替える必要がないことが、ユーザー空間スケジューリングの要点です。CPUレジスタを直接保存せず、Pythonが保持するgeneratorの実行状態（local変数、停止位置、評価状態）を利用します。停止中は `generator.gi_frame` からframeを観察できます。testにlocal変数の保持と完了後のframe解放の例があります。別threadで実行中のframeを読んではいけません。

## Phase 3: 100,000 Taskとメモリ

```sh
uv run python -m examples.m1_many --tasks 100000
uv run python -m benchmarks.compare --suite tasks --repeats 3 > benchmark-results-tasks.jsonl
```

全Taskをqueueに登録してから実行し、`completed == tasks` とqueueの空を検証します。完了Taskはruntimeのactive集合から除きます。呼び出し側がhandleを保存すれば、そのTaskとresultは保持されます。many demoはhandleを保持しません。

比較runnerはOS thread・M:1・asyncioを同じ小さなTask数でも測定し、M:1・asyncioは10万個でも測定します。`--thread-tasks`（上限512）と `--green-tasks` で変更できます。これは最大起動可能数の証明ではなく、**指定した数で完走できるか**の測定です。

| JSON field | 読み方 |
| --- | --- |
| `tasks`, `started`, `completed` | 要求数、OS thread起動成功数、完了数 |
| `spawn_s` | 計測開始から登録/生成完了まで。各方式の初期化も含む |
| `elapsed_s` | 初期化・登録・実行・終了までのwall time。process起動/importは除く |
| `process_cpu_s` | 同区間のprocess全体のCPU時間 |
| `cpu_to_wall` | CPU時間 / wall time。CPU workloadで約1ならおおむね1 core分 |
| `peak_rss_bytes` | process生涯のRSS peak。Linux/macOSで取得、非対応OSではnull |
| `baseline_peak_rss_bytes` | 測定開始時点のRSS peak |

RSSは `resource.getrusage()` を使い、LinuxのKiB / macOSのbytesをbytesへ統一しています。Python heapだけを測る値ではなく、native threadの領域やinterpreterも含みます。peak同士の差は厳密なTaskメモリ量ではなく、現在のRSSでもありません。stack予約量やkernel側メモリは測定していません。

比較は毎回fresh processを使用します。asyncioはhandleと結果を `gather()` のため保持し、OS threadはEventを待ち、M:1は1回yieldします。提供機能と待機機構が違うため、純粋なcontext switch単価の比較ではありません。固定の性能値はここには記載せず、手元のJSONを比較してください。

## Phase 4: M:N

```python
from runtime import MNRuntime, gosched

runtime = MNRuntime(workers=4)

def work():
    for _ in range(10):
        yield gosched()

handles = [runtime.go(work()) for _ in range(1000)]
runtime.start_runtime()
assert runtime.completed == 1000
print(sum(task.migrations for task in handles))
```

```text
M user Tasks → shared deque
                  ↓  ↓  ↓
                 W0 W1 W2   ← 各WorkerはOS thread
```

`Condition` のlockでqueue、active集合、Taskの状態、完了数を一緒に保護します。Taskをqueueから取り出してRUNNINGにする操作は不可分です。`next()` / `throw()` の間はlockを離し、そのWorkerだけがTaskを所有します。`yield`の後に再びlockを取り、末尾へ戻します。同じgeneratorを同時に実行することはありません。

戻ったTaskを別Workerが取得できるため、`last_worker` / `migrations` で移動を観測できます。ただし移動やWorker間の均等配分は保証しません。M:Nの出力順は非決定的です。queueが空でもactive TaskがあればConditionで待機し、全Taskの完了までWorkerを維持します。GILを排他制御の代わりには使っていません。

Taskは登録時に `contextvars.copy_context()` を保持し、再開時に `Context.run()` を使います。`threading.local()` はWorkerに属するのでTaskごとの状態には使えません。yieldをまたぐthread所有の `RLock` 保持、threadに固定された外部APIの利用も避けてください。

## Phase 5: CPU benchmarkとGIL

```sh
PYTHON_GIL=1 uv run python -m examples.m1_cpu_bound --tasks 8 --iterations 5000000
PYTHON_GIL=1 uv run python -m examples.mn_cpu_bound --tasks 8 --iterations 5000000 --workers 4
PYTHON_GIL=1 uv run python -m benchmarks.compare --suite cpu --iterations 5000000 --workers 1 2 4 --repeats 3 > benchmark-results-cpu-gil-on.jsonl
```

ここではfree-threaded build上でGILを明示的に有効化します。次の節では同じinterpreterのGILを無効にし、Pythonのversionやbuildを変えずに比較します。

全方式が同じpure-Pythonの整数二乗和を計算し、数式による期待値と照合します。`--chunk` ごとにyieldします（単体CPU CLIで指定、デフォルト50,000）。小さすぎるchunkはqueue/lockコストを増やし、大きすぎるchunkはcooperativeな応答を悪化させます。runnerは各sampleをstdoutのJSON Linesへ、median時間と `speedup_vs_m1` をstderrへ出力します。

通常のCPythonでGILが有効なら、同一processのPython bytecodeは同時に複数threadで実行できません（[threading公式解説](https://docs.python.org/3/library/threading.html#gil-and-performance-considerations)）。M:NのWorkerを増やしても、この計算のCPU並列性は増えません。`gil_enabled`、速度比、`cpu_to_wall` を合わせて確認します。小さい速度差はCPU周波数・負荷・計測誤差などでも生じるため、「M:Nが必ず遅い」をtestのassertにはしません。

GILのthread切り替えはOS thread間の実行を譲る機構であり、自作queueがTaskをpreemptすることとは別です。`time.sleep()` はGILを解放しても、そのWorker自体は停止します。C extensionがGILを解放する計算やmultiprocessingは、このpure-Python benchmarkの対象外です。

### free-threaded CPython

検出は以下で行えます。buildが対応しているかと、**実行中のGILが無効か**を分けて確認します。

```sh
uv run python -VV
uv run python -c 'import sys,sysconfig; print("build:", sysconfig.get_config_var("Py_GIL_DISABLED")); print("GIL enabled:", sys._is_gil_enabled())'
```

buildが `1` ならfree-threaded対応です。次のコマンドでGILを切り替えてtestとCPU benchmarkを実行します。benchmarkは負荷が干渉しないよう、順番に実行してください。

```sh
PYTHON_GIL=0 uv run python -m unittest discover -s tests -v
PYTHON_GIL=1 uv run python -m unittest discover -s tests -v
PYTHON_GIL=0 uv run python -m benchmarks.compare --suite cpu --iterations 5000000 --workers 1 2 4 --repeats 3 > benchmark-results-cpu-gil-off.jsonl
PYTHON_GIL=1 uv run python -m benchmarks.compare --suite cpu --iterations 5000000 --workers 1 2 4 --repeats 3 > benchmark-results-cpu-gil-on.jsonl
```

runnerは同じ `sys.executable` と環境変数を子processへ渡します。親の `-X gil=0` は自動継承されないため、runnerでは `PYTHON_GIL` を使います。

GILが無効なら複数Workerが計算を同時実行でき、十分な仕事量・core数があれば `cpu_to_wall > 1` と高速化を期待できます。ただし共有queue、同期、メモリ管理のコストがあり、Worker数に比例するとは限りません。拡張moduleがGILを再有効化する場合もあります。[Python公式のfree-threading解説](https://docs.python.org/3/howto/free-threading-python.html)を参照してください。

通常CPython 3.13.5と、uv管理のCPython 3.14.7 free-threaded buildでの検証記録は[VALIDATION.md](VALIDATION.md)にまとめています。性能値は固定せず、実行したJSONの `gil_enabled`、`cpu_to_wall`、stderrの `speedup_vs_m1` を比較してください。free-threaded buildでGILを有効化した場合と、通常buildそのものの性能は同一とは限りません。

## Phase 6: I/O待ちをWorkerから分離

```sh
uv run python -m examples.netpoller_demo --mode m1 --sockets 100
uv run python -m examples.netpoller_demo --mode mn --sockets 100 --workers 2
uv run python -m examples.blocking_demo --mode m1 --tasks 32
uv run python -m examples.blocking_demo --mode mn --tasks 32 --workers 4
```

netpoller demoはsocketpairを接続ごとに作り、reader Taskが1 byteを待ちます。1つのproducer threadが全readerの開始後に短時間待ち、外部peerを模擬して一括送信します。接続ごとのthreadは作りません。`peak_waiting` がWorker数を超え、全Taskが完了することを確認してください。socketごとに2個のfdが必要なので、数を増やす際はfd上限にも影響されます。

I/O有効時のthread数は、M:1では呼び出し元thread + poller、M:Nでは呼び出し元のjoin待ちthread + N Worker + pollerです。demoではこれにproducerが1本加わります。「M:1」はユーザーTaskを実行するthreadが1本という意味です。

blocking demoではTask内の通常の `time.sleep()` がWorkerを止めます。おおむねTask数/Worker数に応じた回数の待ちが必要です。netpollerではI/OがまだreadyでなくてもWorkerが別Taskを実行できます。両demoは模擬workloadが異なるため、厳密なthroughput比較ではありません。

```text
READY ── dequeue ──→ RUNNING ── yield gosched() ──→ READY
                        │
                        ├─ yield WaitIO ──→ WAITING
                        │                     │
                        │              selector ready / timeout
                        │                     └──────────→ READY
                        ├─ return ──→ DONE
                        └─ unhandled exception ──→ FAILED
runtimeの中断で残ったTask ──→ CANCELLED
```

`enable_io=True` でpollerを有効化します。WorkerはWAITINGを設定してから登録要求をQueueへ送り、socketpairでpollerを起こします。Selectorのregister/unregister/selectはpoller threadだけが実行します。ready時に登録を解除してからTaskをqueueへ戻すone-shot方式です。timeoutや登録失敗は次の再開で `generator.throw()` し、Task側でcatchできます。[selectors公式仕様](https://docs.python.org/3/library/selectors.html)に沿って、OSごとのDefaultSelectorを使用します。

```python
from runtime.io import recv, send_all

def exchange(sock):
    sock.setblocking(False)
    yield from send_all(sock, b"hello", timeout=5)
    response = yield from recv(sock, 4096, timeout=5)
    return response
```

readinessは「次の操作が進められる可能性」の通知で、データ全量の転送完了ではありません。helperは `BlockingIOError` なら再登録し、partial sendを繰り返し、recvの `b''` はEOFとして返します。recvは指定byte数までを1回読むAPIであり、メッセージ全体の組み立ては呼び出し側の責務です。helperのtimeoutはI/O操作の待機予算で、各再登録で残り時間を渡します。

## Phase 7: 比較

| 対象 | Scheduling | Context switching | Stack | OS threads | CPU parallelism | I/O multiplexing |
| --- | --- | --- | --- | --- | --- | --- |
| 自作M:1 | 協調round-robin | next / yield、Python実行状態 | stackless | Task実行1、I/O有効時poller追加 | Task間の並列実行なし | 明示的WaitIO + selectors |
| 自作M:N | 協調、共有queue | 同上、Worker移動可 | stackless | N Worker、I/O有効時poller追加 | 通常CPythonはGIL制約、free-threadedなら可能 | 同上 |
| asyncio | event loopによる協調実行 | await、coroutineの再開 | stackless | 基本1 loop thread、executorは別 | 同一loop内は並列なし | loop/backendが処理、OS・loopに依存 |
| threading | OSによるpreemption | OSのthread context切り替え | native stackあり | 原則1 task = 1 thread | 通常CPythonのPython計算はGIL制約 | 自動統合なし、通常のblocking I/Oはthreadを待機 |
| Go goroutine | G/M/P、queue、work stealing、preemption | runtimeが実行contextを切り替え | stackful、伸長可能 | M:N、runtimeが管理 | GOMAXPROCS・利用core等の制約下で可能 | runtimeのnetpollerが対応network I/Oと統合 |
| 元記事のRust実装 | 協調M:1 / 共有queue M:N | assemblyによるregister・stack切り替え | stackful | 1またはN Worker | M:Nで可能 | 未実装、blocking処理はWorkerを停止 |

GoのG/M/Pとruntimeの構成は[Go runtime開発資料](https://go.dev/src/runtime/HACKING)、[asyncio Taskの仕様](https://docs.python.org/3/library/asyncio-task.html#task-object)、Pythonのyieldの意味は[Python言語reference](https://docs.python.org/3/reference/expressions.html#yield-expressions)を参照してください。GoのすべてのI/Oが必ずnonblockingになるわけではなく、ここではruntimeに統合されたnetwork I/Oを比較しています。

### 再現できた部分 / できない部分

- 再現: 軽量Taskの大量登録、run queue、協調的な実行権譲渡、M:1 / M:N、Worker別local queueとwork stealing、Worker間のTask移動、I/O待ちと実行の分離。
- 置き換え: 専用native stackとregister保存の代わりにgeneratorとPython実行状態を利用。元記事のcontext switchコストそのものは測れません。
- 非再現: 普通の関数の任意の深さから透過的に停止するstackful coroutine。Python版では停止可能な呼び出しを `yield from` でつなぎます。
- 非再現: 強制preemption、伸長するnative stack、Goと同等のG/M/P・複数Task単位のsteal・blocking syscallの自動変換。Pure Pythonから任意の処理を安全に中断する機能はありません。
- 制約: M:Nの構造を再現しても、通常CPythonではpure-Python CPU計算のマルチコア並列性は得られません。

## Phase 8: Local Queue + Work Stealing

shared queue版の`MNRuntime`は、すべてのyield・取得で1本のqueueを共有します。設計が明快な一方、free-threaded PythonでWorkerが同時実行すると同じlockへのアクセスが集中します。比較用の`MNRuntime`はそのまま残し、scheduling policyを比較する`WorkStealingRuntime`を追加しました。

```text
                  Global Queue
                       │
              ┌────────┴────────┐
              ▼                 ▼
          Local Q 0         Local Q 1
              │                 │
          Worker 0  ← steal → Worker 1
```

Workerは自分のlocal queue、global queue、他Workerのlocal queueの順に探します。root Taskとtimer/I/Oから復帰したTaskはglobal queueへ入り、Taskがyieldした場合とdynamic spawnしたchildは現在のWorkerのlocal queueへ入ります。stealはvictimの反対側から1 Taskずつ取得します。すべてのqueueとTask lifecycleは同じ`Condition`で保護し、仕事がなければbusy loopせず待機します。

このlocal queueはWorkerごとに分かれていますが、**local queueを含む全queue操作とTask lifecycleは共通の`Scheduler._condition`配下**です。したがって、この実装が再現するのはTask locality、負荷の偏り、stealによるWorker間のTask再配置というpolicyです。global lock contentionの大幅な削減や、lock競合を抑えるqueue構造そのものは再現していません。Go runtimeはProcessorごとのrun queueに対して、より細かな同期と高度なqueue構造を利用します。

```sh
uv run python -m examples.work_stealing_demo --workers 4 --tasks 10000
PYTHON_GIL=0 uv run python -m benchmarks.compare --suite cpu --workers 1 2 4 --iterations 5000000 --repeats 3 > benchmark-results-work-stealing.jsonl
PYTHON_GIL=0 uv run python -m benchmarks.compare --suite stealing --workers 1 2 4 --tasks 24 --heavy-tasks 6 --iterations 1000000 --light-iterations 100000 --repeats 3 > benchmark-results-stealing.jsonl
```

`benchmarks.stealing`はroot Taskを均等投入せず、parentがheavy/light childをdynamic spawnします。複数Worker時は最初のchildがowner Workerで待機し、別Workerがparentを取得してspawnを進める偏りを意図的に作ります。WorkStealingRuntimeではこの進行に実stealが必要なので、`steals_succeeded`が増えます。shared queue版M:Nと各Worker数をfresh processで実行し、JSONの`elapsed_s`、`process_cpu_s`、`cpu_to_wall`、完了数、migration、queue/steal統計を比較できます。

このbenchmarkの目的は、負荷不均衡時にstealが起き、TaskがWorker間へ再配置されることの観察です。shared queueとwork stealingのどちらが常に高速というわけではありません。結果はworkloadの偏り、Task粒度、Worker数、共通queue lock、Python runtimeによって変わります。現在のWorkStealingRuntimeは共通`Condition`を使うため、Go runtimeと同じlock contention特性を測るbenchmarkではありません。

Go runtimeはProcessorごとのrun queue、global queue、複数Task単位のsteal、syscall・netpoller・preemptionとの統合を持ちます。この実装はlocalityと負荷分散を観察する最小モデルで、Processorや強制preemptionはありません。

## Phase 9: Timer Queue / cooperative sleep

Task内の`time.sleep()`はそのTaskを実行しているWorkerごと止めます。`yield sleep(seconds)`はTaskをWAITINGへ移し、Workerを別Taskへ返します。

```python
from runtime import sleep

def task():
    yield sleep(0.1)
    print("ready")
```

```sh
uv run python -m examples.sleep_demo
```

初回のSleepで専用timer threadを起動します。`heapq`のmin-heapへ`(deadline, sequence, task)`を積み、最短deadlineまで待ち、期限到達後にTaskをREADYとしてrun queueへ戻します。`sleep_waits`と、demoで`sleeper: end`より先にrunnable Taskが完了する順序を観測できます。M:1でもTask実行Workerはblockingしません。

I/O timeoutは現在もNetPoller内で管理しています。sleepとI/O timeoutを1つのtimer heapへ統合するとcancel tokenやregistration世代の管理が必要になるため、教材として責務を分離しました。Go runtimeではtimer、scheduler、netpollerがより密接に連携し、多数のtimerとnetwork待ちを扱います。

## Phase 10: Dynamic Task Spawn

start前の`runtime.go()`だけでは、実行結果に応じた並行処理を作れません。Task内で`yield spawn(generator)`を行うと、Schedulerがchildをactive集合へ追加し、parentとchildをともにREADYへ戻します。

```python
from runtime import spawn

def child():
    yield
    return 42

def parent():
    child_task = yield spawn(child())
    print("child id:", child_task.id)
```

```sh
uv run python -m examples.spawn_demo
```

yield式の戻り値はchildのTask handleです。childはparentの`contextvars.Context`をcopyし、重複しないTask idを受け取ります。active Task数にはchild・grandchildも含まれるため、runtimeは動的に生成された全TaskがDONE/FAILEDになるまで終了しません。childの通常の`Exception`はそのchildだけをFAILEDにし、最後に`TaskErrors`へ集約します。

M:1とshared queue M:Nではparent・childをshared queue末尾へ置きます。WorkStealingRuntimeでは現在Workerのlocal queueへ置くため、他Workerがstealできます。外部threadから実行中の`go()`を呼ぶAPIは提供していません。Goの`go`文は通常関数をstackful goroutineとして起動できますが、この実装ではgeneratorを明示し、spawn自体もyield地点になります。

## APIの範囲と終了処理

学習用の一回限りのbatch runtimeです。root Taskをstart前に登録し、実行中は`yield spawn(...)`でchildを追加できます。外部threadからの実行中`go()`、再実行、個別cancel、Task完了を待つjoin、channel、DNS/TLS/accept/connect helperは実装していません。

`Exception`は`task.error`に保持し、他Taskを続行した後に`TaskErrors`を呼び出し元へ返します。`failures`で失敗Taskを参照できます。KeyboardInterruptやSystemExitなど`Exception`ではない`BaseException`はruntime全体をabortし、Worker・poller・timerを停止して残るgeneratorをcloseした後、元の例外をそのまま伝播します。終了Taskはqueueとactive集合から除外されますが、失敗Taskのtracebackは診断のため保持します。Task handleのstate/result/statisticsは`start_runtime()`の終了後に参照してください。

`start_runtime(timeout=...)` は**yield境界で確認する協調的deadline**です。poller障害や期限超過ではWorkerを停止させ、pollerをjoin・closeし、残るgeneratorをcloseします。停止しない無限loop、blocking socket、停止しないfinallyを強制終了できません。厳密な実行時間上限が必要な実験は別processのtimeoutで保護してください。I/O timeoutなしでpeerが永遠に沈黙すれば、待ち続けるのは仕様です。

同じsocketで同時に待てるTaskは1個だけです（read/write同時waitも未実装）。同一方向の複数waitも拒否します。方向別waiterへ拡張するにはSelectorのevent mask更新と方向ごとのtimeout/cancel管理が必要なため、P2候補として残しました。socketは呼び出し側が所有し、待機中に別threadからcloseせず、完了後にcloseしてください。fdの外部close・再利用を完全に追跡する設計ではありません。netpollerのtimeout検索は学習のため線形走査で、巨大なI/O timeout集合に最適化していません。

`sleep()`、`wait_read()`、`wait_write()`、`recv()`、`send_all()`のtimeout/delayは、有限の非負数または`None`に統一しています。負数、NaN、inf、bool、数値以外を拒否します。socket操作が即時成功できる場合も、helperは操作前にvalidationします。`start_runtime(timeout=...)`だけは有限の正数が必要です。

## 検証

```sh
uv run python -m unittest discover -s tests -v
uv run python -m compileall -q src examples benchmarks tests
```

testsは従来の完了・再開・round-robin・10万Task・M:N・I/Oに加え、BaseException abort、generator/thread cleanup、deadline順sleep、dynamic spawn、Task id、work stealingの実発生、timeout validationを確認します。性能に環境依存の閾値は設定していません。

GitHub Actionsは通常CPython 3.11〜3.14のmatrixと、CPython 3.14 free-threaded版のGIL無効/有効matrixでunittestとcompileallを実行します。free-threaded版は`actions/setup-python`の`3.14t`を利用します。

benchmarkは同時に走らせず、他のCPU負荷を避けて反復してください。benchmark結果はgit対象外のJSON Linesへ保存できます。今回実行した内容と未検証範囲は `VALIDATION.md` に記録しています。
