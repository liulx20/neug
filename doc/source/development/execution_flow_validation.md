# 执行流端到端性能验收（2026-09-15）

本轮只比较 `e614c9d4` 与 `fd868b41`，即 Unfold、Primary-key Join 接入 worker pipeline 的最近一次改动。对照查询中的图扩展、普通 Join、GroupBy、Dedup 在修改前已经支持并行，不能把它们的多线程收益归到这次提交。

## 结论

- 百万行 Unfold、四倍展开后聚合：4 worker 约 451 → 164 ms，耗时下降 63.6%；1 worker 基本持平。
- 真实索引上的物理 PK Join：4 worker 约 69 → 31 ms，耗时下降 55.7%；1 worker 基本持平。
- 通用 Join 在首轮 2 worker 出现 +6.5%，单独增加样本后为 -0.2%，没有复现稳定退化。保留两轮数据，不删掉首轮异常。
- 混合长短查询的中位数和 p95 基本持平，但两版仍有超过 100 ms 的个别短查询尾延迟。本轮没有定位这些尾延迟的来源。

## 口径

本机 macOS ARM64、10 个物理核、16 GiB，Release，HTTP server 开启，native-arch 关闭。两版使用相同源码基线和构建配置，仅切换这次提交涉及的三个执行器源文件重建核心库。每个进程通过动态加载日志验证实际加载的 libneug，产物 SHA-256 保存在原始数据中。

数据为 1,000,000 个 item、200,000 个 dim、2,000,000 条 link 边。item 的 k 为 id % 200000，每个 item 连到 (id+1)%N 和 (id+7)%N。两版读取同一份 checkpoint，导入和打开数据库均不计时。

查询先 PROFILE 校验算子路径和结果，再对每个 worker 数预热。正式计时关闭 profiling，使用 `perf_counter` 测量 `execute` 到 `list(result)` 完成的墙钟时间，包括调度、执行和读取返回值；编译、预热、结果正确性断言均在计时外。批量查询最终都聚合为一行，**不代表导出百万行到 Python 的耗时**。

每种模式按 A/B/B/A/A/B/B/A 启动独立进程，每进程每个查询/worker 配置取 5 个样本。表中为四个进程各自样本中位数的中位数。PROFILE 只用于确认路径，其并行算子时间不能相加当作查询墙钟耗时。

## 完整查询耗时

单位 ms；列内依次为 1 / 2 / 4 worker。

| 查询 | 修改前 | 修改后 | 修改后 1→4 加速比 |
|---|---:|---:|---:|
| scan_project | 67.168 / 32.539 / 18.917 | 68.830 / 32.768 / 18.467 | 3.73× |
| unfold | 433.279 / 442.900 / 450.759 | 428.402 / 233.490 / 164.090 | 2.61× |
| expand | 94.667 / 46.772 / 24.715 | 94.506 / 47.120 / 25.318 | 3.73× |
| join | 248.644 / 126.096 / 70.283 | 251.793 / 134.278 / 71.735 | 3.51× |
| dedup | 198.967 / 119.370 / 74.206 | 203.662 / 117.953 / 76.206 | 2.67× |
| group | 92.561 / 45.314 / 29.350 | 88.917 / 46.164 / 28.114 | 3.16× |
| short | 0.017 / 0.043 / 0.037 | 0.020 / 0.037 / 0.036 | 0.55× |

查询文本和解析出的算子列表保存在脚本及原始 JSON 中。expand 命中 EdgeExpandVOpr；join 命中 JoinOpr；unfold 命中 UnfoldOpr。short 是单点主键查询，额外 worker 没有带来收益，微秒级百分比也很容易受噪声影响。

## PK Join 物理 pipeline

独立 C++ 程序使用数据库的真实顶点索引，显式构造 PrimaryKeyJoinOpr，右侧 Project 将预制输入映射为查找键。百万输入按 4096 行一批，全部命中；复用四线程 TaskPool，每次按 1/2/4 限制查询并行度。计时包括 ExecuteReader、主键查找和 materialize，输入列构造及逐行核对原始主键不计时。它不包含 compiler 和扫描源，也不模拟普通查询必然选中 PK Join。

| worker | 修改前 ms | 修改后 ms | 耗时变化 |
|---|---:|---:|---:|
| 1 | 50.417 | 50.369 | -0.1% |
| 2 | 61.120 | 33.716 | -44.8% |
| 4 | 69.076 | 30.603 | -55.7% |

Unfold 与 PK Join 修改前都在 worker 段外执行，增加 worker 不能并行它们自身，且可能增加边界交接成本。此次将逐 chunk 的工作接入 worker 段，结果与代码改动一致。但本轮没有分别计量复制、锁等待和内存带宽，不能据此归因剩余耗时或断言 4 worker 应线性加速。

## 通用 Join 复测

首轮 2 worker 为 126.096 → 134.278 ms（+6.5%）。随后仅运行同一条 Join 查询，仍采用八进程 ABBA 顺序，每配置增加到 15 个样本。

| worker | 修改前 ms | 修改后 ms | 耗时变化 |
|---|---:|---:|---:|
| 1 | 251.831 | 252.549 | +0.3% |
| 2 | 128.202 | 127.989 | -0.2% |
| 4 | 72.344 | 70.256 | -2.9% |

复测未支持固定的 6.5% 代码退化。首轮变化可能包含机器状态和运行顺序的影响，尚无证据把它归为任务过细、复制或分区倾斜。

## 长短查询混跑

共享四线程池，四条独立连接：一条循环运行 30 次 GroupBy，另外三条持续运行主键点查，直到长查询循环结束。下表仍为四个独立进程指标的中位数，p95 是每进程请求样本的 p95；“最大值”列是四个进程最大值的中位数，不是统一请求池的最大值。

| 指标 | 修改前 ms | 修改后 ms |
|---|---:|---:|
| long median_ms | 45.1732 | 45.8825 |
| long p95_ms | 52.9425 | 53.0322 |
| long max_ms | 60.1201 | 59.0590 |
| short median_ms | 0.0364 | 0.0360 |
| short p95_ms | 0.0515 | 0.0522 |
| short max_ms | 130.2606 | 127.0259 |

本轮未测独立的队列等待时间；端到端短查询耗时也包含线程调度和 Python 客户端开销。尾延迟问题在修改前后都存在，不能据此认定是此次两个算子的改动引入。

## 复现与范围

- [完整查询脚本](benchmarks/execution_flow_validation.py)
- [ABBA 启动脚本](benchmarks/run_execution_flow_abba.py)
- [完整原始结果](benchmarks/execution_flow_abba.json)
- [Join 复测原始结果](benchmarks/execution_flow_join_recheck.json)
- C++ 物理路径程序：`tools/benchmarks/pk_join_pipeline.cc`，构建目标 `pk_join_pipeline`。

```bash
cmake --build build --target pk_join_pipeline -j4
PYTHONPATH=tools/python_bind python doc/source/development/benchmarks/execution_flow_validation.py /tmp/flow/db --prepare
python doc/source/development/benchmarks/run_execution_flow_abba.py /tmp/flow/db /tmp/before /tmp/after /tmp/results.json --native build/tests/execution/pk_join_pipeline
python doc/source/development/benchmarks/run_execution_flow_abba.py /tmp/flow/db /tmp/before /tmp/after /tmp/join.json --query join --repeats 15
```

before/after 目录分别放对应构建的 `libneug.dylib` 和 `tools/python_bind/neug_py_bind*.so`。启动脚本目前使用 macOS 动态库加载环境变量。Python 解释器须已安装项目依赖。

所有计时样本都核对结果，24 个主测进程和 8 个复测进程通过；C++ 物理基准编译通过。没有修改生产执行器逻辑。这里覆盖均匀整数键、固定出度、全部命中的 PK Join，以及暖缓存运行；未覆盖强倾斜、复杂键、索引未命中分布、冷缓存或超内存数据，不将结果泛化为所有查询都能加速。
