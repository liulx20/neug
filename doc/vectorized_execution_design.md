# Neug 向量化执行引擎重构设计文档

## 1. 背景与动机

### 1.1 当前执行模型

Neug 当前采用一种**全量 Context 传递**的执行模型：

- `IOperator::Eval(graph, params, Context&& ctx)` 接收整个 Context（包含所有行），返回新的 Context
- `Pipeline::Execute` 是简单的 for 循环，依次调用每个 operator 的 Eval
- `Context` 是列式存储结构（`IContextColumn` 的集合），通过 `tag_id`/`alias` 索引列
- 1:N 展开通过 `reshuffle`（offset 数组）机制实现：新列追加到 Context，旧列按 offset 重排对齐

核心代码路径：

```
Pipeline::Execute
  └── for each operator:
        ctx = operator->Eval(graph, params, std::move(ctx), timer)
```

关键接口定义（`include/neug/execution/execute/operator.h`）：

```cpp
class IOperator {
  virtual neug::result<Context> Eval(IStorageInterface& graph,
                                     const ParamsMap& params,
                                     Context&& ctx,
                                     OprTimer* timer) = 0;
};
```

### 1.2 当前模型的问题

| 问题 | 说明 |
|------|------|
| **Cache 不友好** | Context 一次性携带全部行数据，中间结果可能远超 L2/L3 cache 容量 |
| **无法流水线并行** | 每个算子必须处理完全部数据才能交给下一个算子，无法让上下游算子同时工作 |
| **内存峰值高** | EdgeExpand 等 1:N 算子一次性展开所有结果，中间结果集可能非常大 |
| **无法多线程** | 单一 Context 对象在算子间传递，天然单线程 |

### 1.3 目标

仿照 DuckDB 的向量化执行引擎，将 Neug 的执行模型改造为：

- **批量（Chunk-based）** 处理：固定大小的数据批次在算子间流动
- **Push-based Pipeline**：数据从 Source 推向 Sink，pipeline breaker 处切断
- **图算子原生支持**：EdgeExpand、PathExpand 等图特有算子在向量化框架下高效运行
- **可扩展到并行执行**：设计上预留多线程 pipeline 执行的能力

---

## 2. DuckDB 向量化执行模型概述

DuckDB 的执行引擎是本次重构的主要参考，其核心概念如下。

### 2.1 DataChunk

`DataChunk` 是数据流动的基本单位，表示一个固定大小（默认 2048 行）的列式数据批次：

```cpp
class DataChunk {
    vector<Vector> data;   // 多个列向量
    idx_t count;           // 当前有效行数（<= STANDARD_VECTOR_SIZE）
};
```

### 2.2 三角色算子模型

DuckDB 将物理算子分为三种角色：

```
┌──────────┐     DataChunk      ┌──────────┐     DataChunk      ┌──────────┐
│  Source   │ ──────────────────>│ Operator │ ──────────────────>│   Sink   │
│ (产生数据) │                    │ (转换数据) │                    │ (消费数据) │
└──────────┘                    └──────────┘                    └──────────┘
    GetData()                      Execute()                      Sink()
                                                                  Finalize()
```

- **Source**：产生数据（如表扫描），每次调用 `GetData()` 输出一个 chunk
- **Operator**：1:1 转换数据（如 Filter、Project），接收一个 chunk 输出一个 chunk
- **Sink**：需要看到全部数据才能产出结果（如 OrderBy、HashJoin），是 **pipeline breaker**

### 2.3 Pipeline 结构

一条 Pipeline 的执行循环：

```
while (source.GetData(chunk) != FINISHED) {
    for (auto& op : operators) {
        op.Execute(chunk, output);
        chunk = output;
    }
    sink.Sink(chunk);
}
sink.Finalize();
```

Pipeline Breaker 处切断 pipeline，形成 pipeline DAG：

```
Pipeline 1: Scan → Filter → [HashJoin Build Side Sink]
Pipeline 2: Scan → [HashJoin Probe Side] → Project → [OrderBy Sink]
Pipeline 3: [OrderBy Source] → Limit → [Result Sink]
```

### 2.4 状态管理

DuckDB 通过分离的状态对象支持并行：

- `GlobalState`：所有线程共享（如 hash table）
- `LocalState`：每个线程独有（如 local buffer），Combine 时合并到 Global

---

## 3. 图数据的特殊性分析

在设计 Neug 的向量化执行引擎之前，必须理解图查询与关系查询的关键差异。

### 3.1 图特有的数据类型

Neug 的 `Context` 中存在关系数据库不具备的列类型：

| 列类型 | 说明 | 当前实现 |
|--------|------|----------|
| `VertexColumn` | 顶点引用 (label, vid) | `SLVertexColumn`（单标签）、`MLVertexColumn`（多标签）、`MSVertexColumn`（多段） |
| `EdgeColumn` | 边引用 (src, dst, label, eid) | `EdgeColumn` |
| `PathColumn` | 变长路径（顶点+边序列） | `PathColumn`，内部为 `Path` 对象数组 |
| `ValueColumn` | 标量属性值 | 基于 Arrow 的列式存储 |

### 3.2 EdgeExpand 的 1:N 展开特性

EdgeExpand 是图查询中最核心、最频繁的操作。其本质是：对输入的每个顶点，查找其邻居，产出 0 到 N 条结果。

```
输入:  [v1, v2, v3]       （3 行）
输出:  [v1→u1, v1→u2, v2→u3, v2→u4, v2→u5, v3→u6]  （6 行）
```

关键挑战：
- **输出行数不可预测**：一个顶点可能有 0 个邻居，也可能有数万个
- **reshuffle 机制**：输出中 v1 对应 2 行、v2 对应 3 行，其他列需要按照 offset `[0,0,1,1,1,2]` 重排
- **数据倾斜**：社交网络中的超级节点可能导致某个输入顶点的展开结果远超一个 chunk

### 3.3 PathExpand 的迭代特性

PathExpand 是多跳路径遍历（如 1..5 跳），本质是迭代式的 BFS/DFS：

```
输入:  [v1]
1跳:   [v1→u1, v1→u2]
2跳:   [v1→u1→w1, v1→u1→w2, v1→u2→w3]
...
结果:  满足 hop_lower..hop_upper 的所有路径
```

关键挑战：
- **迭代次数动态**：由 `hop_lower` 和 `hop_upper` 决定
- **中间状态巨大**：frontier 在每一跳后可能指数级膨胀
- **路径去重**：`PathOpt::kSimple` 要求路径中无重复顶点
- **shortest path 变体**：需要维护距离信息和 visited 集合

---

## 4. 核心数据结构设计

### 4.1 GraphVector

`GraphVector` 是向量化执行的列级数据单元，是对现有 `IContextColumn` 的 batch 化封装。

```cpp
enum class GraphVectorType {
    kFlat,           // 连续存储，所有值在 data array 中
    kConstant,       // 所有行相同值（如常量投影）
    kDictionary,     // 引用另一个 vector + selection vector（零拷贝过滤）
};

class GraphVector {
public:
    GraphVector(DataType type, idx_t capacity = STANDARD_VECTOR_SIZE);

    DataType type_;
    GraphVectorType vector_type_;
    idx_t count_;

    // 根据 DataType 选择底层存储
    // kVertex: label_t[] + vid_t[]
    // kEdge:   src_vid_t[] + dst_vid_t[] + edge_label_t[] + eid_t[]
    // kValue:  类型化的 flat array (int64_t[], double[], string[], ...)
    // kPath:   offset_t[] + child PathNode[]（变长）
    void* data_;

    // 用于 kDictionary 模式的 selection vector
    SelectionVector sel_;

    // Validity mask: 标记 NULL 值
    ValidityMask validity_;
};
```

各类型的具体布局：

```
VertexVector (Flat):
  ┌─────────────────────────┐
  │ label_t[2048]           │   顶点标签数组
  │ vid_t[2048]             │   顶点 ID 数组
  └─────────────────────────┘

EdgeVector (Flat):
  ┌─────────────────────────┐
  │ label_t[2048]           │   边标签
  │ vid_t[2048]             │   源顶点
  │ vid_t[2048]             │   目标顶点
  │ eid_t[2048]             │   边 ID
  └─────────────────────────┘

PathVector (Flat, 变长):
  ┌─────────────────────────┐
  │ offset_t[2049]          │   每条路径在 child 中的起止位置
  │ PathNode[...]           │   所有路径节点顺序存储
  └─────────────────────────┘
  （类似 DuckDB 的 LIST 类型布局）

ValueVector (Flat):
  ┌─────────────────────────┐
  │ T[2048]                 │   类型化数组（int64_t / double / ...）
  └─────────────────────────┘
```

### 4.2 SelectionVector

用于零拷贝过滤，避免在 Filter 操作时物理移动数据：

```cpp
class SelectionVector {
public:
    SelectionVector(idx_t count);

    idx_t get_index(idx_t i) const { return sel_[i]; }
    void set_index(idx_t i, idx_t val) { sel_[i] = val; }

    idx_t* data() { return sel_; }

private:
    idx_t* sel_;        // 索引数组
    idx_t capacity_;
};
```

### 4.3 GraphDataChunk

`GraphDataChunk` 是算子间数据流动的基本单位：

```cpp
static constexpr idx_t STANDARD_VECTOR_SIZE = 2048;

class GraphDataChunk {
public:
    void Initialize(const vector<DataType>& types,
                    idx_t capacity = STANDARD_VECTOR_SIZE);
    void Reset();

    idx_t size() const { return count_; }
    idx_t ColumnCount() const { return data_.size(); }

    GraphVector& GetVector(idx_t col_idx) { return data_[col_idx]; }
    void SetCardinality(idx_t count) { count_ = count; }

    // 通过 selection vector 做零拷贝切片
    void Slice(const SelectionVector& sel, idx_t count);

    // 追加另一个 chunk 的数据
    void Append(const GraphDataChunk& other);

private:
    vector<GraphVector> data_;   // 列向量集合
    idx_t count_;                // 当前有效行数
    vector<int> tag_ids_;        // 每列对应的 alias/tag（图查询中列按 tag 引用）
};
```

### 4.4 与现有 Context 的映射关系

```
现有 Context                        新 GraphDataChunk
─────────────────                   ──────────────────
columns: vector<IContextColumn>  →  data_: vector<GraphVector>
head: IContextColumn             →  data_[0]（约定第一列为 head）
tag_ids: vector<int>             →  tag_ids_: vector<int>
row_num(): 全部行数               →  size(): 当前 chunk 行数（<= 2048）
reshuffle(offsets)               →  不再需要：改为算子内部处理
```

---

## 5. 算子接口设计

### 5.1 基类定义

```cpp
enum class SourceResultType { HAVE_MORE_OUTPUT, FINISHED };
enum class OperatorResultType { NEED_MORE_INPUT, HAVE_MORE_OUTPUT, FINISHED };
enum class SinkResultType { NEED_MORE_INPUT, FINISHED };

// ─── Source：产生数据 ───
class IPhysicalSource {
public:
    virtual ~IPhysicalSource() = default;
    virtual string GetName() const = 0;

    virtual unique_ptr<GlobalSourceState> GetGlobalSourceState() const = 0;
    virtual unique_ptr<LocalSourceState> GetLocalSourceState(
        GlobalSourceState& gstate) const = 0;

    virtual SourceResultType GetData(ExecutionContext& ctx,
                                     GraphDataChunk& chunk,
                                     GlobalSourceState& gstate,
                                     LocalSourceState& lstate) = 0;
};

// ─── Operator：1:1 转换 ───
class IPhysicalOperator {
public:
    virtual ~IPhysicalOperator() = default;
    virtual string GetName() const = 0;

    virtual unique_ptr<OperatorState> GetOperatorState() const = 0;

    virtual OperatorResultType Execute(ExecutionContext& ctx,
                                       GraphDataChunk& input,
                                       GraphDataChunk& output,
                                       OperatorState& state) = 0;
};

// ─── Sink：消费全部数据 ───
class ISinkOperator {
public:
    virtual ~ISinkOperator() = default;
    virtual string GetName() const = 0;

    virtual unique_ptr<GlobalSinkState> GetGlobalSinkState() const = 0;
    virtual unique_ptr<LocalSinkState> GetLocalSinkState(
        GlobalSinkState& gstate) const = 0;

    virtual SinkResultType Sink(ExecutionContext& ctx,
                                GraphDataChunk& chunk,
                                GlobalSinkState& gstate,
                                LocalSinkState& lstate) = 0;

    // 所有线程的 LocalSinkState 合并到 GlobalSinkState
    virtual void Combine(GlobalSinkState& gstate,
                         LocalSinkState& lstate) = 0;

    // 单线程调用，所有数据 Sink 完成后的最终处理
    virtual void Finalize(GlobalSinkState& gstate) = 0;
};
```

### 5.2 ExecutionContext

```cpp
class ExecutionContext {
public:
    IStorageInterface& graph;
    const ParamsMap& params;
    // 后续扩展：线程 ID、内存限制、取消信号等
};
```

---

## 6. 算子分类与向量化策略

### 6.1 完整算子分类表

| 算子 | 角色 | Pipeline Breaker | 向量化难度 | 说明 |
|------|------|:-:|:-:|------|
| **Scan** | Source | - | 低 | 按 chunk 扫描顶点/边 |
| **Select/Filter** | Operator | 否 | 低 | 用 SelectionVector 零拷贝过滤 |
| **Project** | Operator | 否 | 低 | 列投影 + 表达式求值 |
| **GetV** | Operator | 否 | 低 | 根据边获取端点，1:1 映射 |
| **Limit** | Operator | 否 | 低 | 计数截断，支持早停 |
| **EdgeExpand** | Source-like | 否 | **高** | 1:N 展开，需带状态的分批输出 |
| **PathExpand** | Sink+Source | **是** | **极高** | 迭代多跳，pipeline breaker |
| **Intersect** | Sink+Source | **是** | 中 | 多路求交，需全量数据 |
| **GroupBy** | Sink+Source | **是** | 中 | hash aggregate |
| **OrderBy** | Sink+Source | **是** | 中 | 全量排序 |
| **Dedup** | Sink+Source | **是** | 中 | hash 去重 |
| **Join** | Sink+Source | **是** | 中 | hash join，build side 是 Sink |
| **Union** | Source | - | 低 | 合并多个 Source |
| **Unfold** | Operator | 否 | 中 | 路径展开为顶点/边序列 |

### 6.2 Scan 算子

最简单的 Source，按 chunk 大小扫描存储层：

```cpp
class VertexScanSource : public IPhysicalSource {
    // State: 当前扫描到的 vertex label 和 vid 偏移
    struct ScanState : public LocalSourceState {
        label_t current_label;
        vid_t current_vid;
    };

    SourceResultType GetData(ExecutionContext& ctx,
                             GraphDataChunk& chunk,
                             GlobalSourceState& gstate,
                             LocalSourceState& lstate) override {
        auto& state = static_cast<ScanState&>(lstate);
        idx_t count = 0;
        auto& vertex_vec = chunk.GetVector(0);  // VertexVector

        while (count < STANDARD_VECTOR_SIZE && !scan_finished(state)) {
            vertex_vec.SetVertex(count, state.current_label, state.current_vid);
            advance(state);
            count++;
        }

        chunk.SetCardinality(count);
        return count > 0 ? SourceResultType::HAVE_MORE_OUTPUT
                         : SourceResultType::FINISHED;
    }
};
```

### 6.3 EdgeExpand 算子（核心）

EdgeExpand 采用**带状态的产出式设计**，类似 DuckDB 的 UNNEST：

```cpp
class EdgeExpandOperator {
    // 不严格属于三种角色之一，而是一个特殊的"产出式算子"：
    // 接收一个 input chunk，可能产出多个 output chunk
    //
    // 返回 HAVE_MORE_OUTPUT 表示当前 input chunk 尚未处理完，
    // 需要继续调用而不需要新的 input

    struct ExpandState : public OperatorState {
        idx_t current_input_row;       // 当前处理到 input 的第几行
        CsrEdgeIterator edge_iter;     // 当前顶点的邻居迭代器
        bool input_exhausted;
    };

    OperatorResultType Execute(ExecutionContext& ctx,
                               GraphDataChunk& input,
                               GraphDataChunk& output,
                               OperatorState& state) {
        auto& s = static_cast<ExpandState&>(state);
        idx_t out_count = 0;

        while (out_count < STANDARD_VECTOR_SIZE) {
            // 如果当前顶点的邻居还没迭代完
            if (s.edge_iter.has_next()) {
                auto [nbr_label, nbr_vid] = s.edge_iter.next();
                // 写入展开结果到 output
                write_expanded_row(output, out_count, input, s.current_input_row,
                                   nbr_label, nbr_vid);
                out_count++;
                continue;
            }

            // 当前顶点邻居迭代完毕，移到下一个输入顶点
            s.current_input_row++;
            if (s.current_input_row >= input.size()) {
                s.input_exhausted = true;
                break;
            }

            // 为下一个顶点初始化邻居迭代器
            auto [label, vid] = input.GetVector(v_tag_).GetVertex(s.current_input_row);
            s.edge_iter = graph.GetNeighborIterator(label, vid, edge_labels_, dir_);
        }

        output.SetCardinality(out_count);

        if (s.input_exhausted) {
            s.Reset();
            return OperatorResultType::NEED_MORE_INPUT;
        } else {
            return OperatorResultType::HAVE_MORE_OUTPUT;
        }
    }
};
```

关键设计要点：
- **不做 reshuffle**：输入列的值在输出时直接复制（或引用），无需全局重排
- **背压（back pressure）**：output chunk 填满即返回，不会一次性展开所有邻居
- **超级节点友好**：一个有百万邻居的顶点会产出多个 chunk，不会撑爆内存

### 6.4 PathExpand 算子

PathExpand 作为 **Pipeline Breaker**，分为 Sink 和 Source 两个阶段。

```
Pipeline N:   ... → [PathExpand.Sink]
Pipeline N+1: [PathExpand.Source] → ...
```

#### Sink 阶段：收集所有源顶点

```cpp
class PathExpandSink : public ISinkOperator {
    struct PathExpandGlobalState : public GlobalSinkState {
        // 收集所有输入的源顶点
        vector<VertexRecord> source_vertices;
        // 展开完成后的结果
        vector<Path> result_paths;
        vector<size_t> result_source_indices;
        // Source 阶段的读取偏移
        atomic<idx_t> source_offset{0};
    };

    SinkResultType Sink(ExecutionContext& ctx, GraphDataChunk& chunk,
                        GlobalSinkState& gstate, LocalSinkState& lstate) override {
        auto& state = static_cast<PathExpandGlobalState&>(gstate);
        // 收集源顶点
        for (idx_t i = 0; i < chunk.size(); i++) {
            auto v = chunk.GetVector(start_tag_).GetVertex(i);
            state.source_vertices.push_back(v);
        }
        return SinkResultType::NEED_MORE_INPUT;
    }

    void Finalize(GlobalSinkState& gstate) override {
        auto& state = static_cast<PathExpandGlobalState&>(gstate);
        // 执行完整的多跳展开（复用现有 BFS/DFS 逻辑）
        ExecutePathExpand(state.source_vertices,
                          state.result_paths,
                          state.result_source_indices);
    }
};
```

#### Source 阶段：分 chunk 输出结果

```cpp
class PathExpandSource : public IPhysicalSource {
    SourceResultType GetData(ExecutionContext& ctx, GraphDataChunk& chunk,
                             GlobalSourceState& gstate,
                             LocalSourceState& lstate) override {
        auto& state = static_cast<PathExpandGlobalState&>(gstate);
        idx_t start = state.source_offset.fetch_add(STANDARD_VECTOR_SIZE);
        idx_t count = min(STANDARD_VECTOR_SIZE,
                          state.result_paths.size() - start);
        if (count == 0) return SourceResultType::FINISHED;

        // 填充 output chunk
        for (idx_t i = 0; i < count; i++) {
            chunk.GetVector(path_alias_).SetPath(i, state.result_paths[start + i]);
            // 关联原始输入列...
        }
        chunk.SetCardinality(count);
        return SourceResultType::HAVE_MORE_OUTPUT;
    }
};
```

#### PathExpand 优化演进路线

```
v1 (Pipeline Breaker):
   收集全部源顶点 → 批量 BFS/DFS → 全量结果输出
   优点: 最简单，可复用现有 path_expand_impl 逻辑
   缺点: 内存峰值高，无流水线

v2 (Frontier-based Iterative):
   维护 frontier chunk 队列，每次 GetData 做一跳展开
   满足 hop 范围的中间结果立即输出
   优点: 流式输出，内存可控
   缺点: 实现复杂

v3 (Decomposed Multi-Pipeline):
   将 K 跳展开拆为 K 个 EdgeExpand pipeline
   中间用 Sink/Source 连接
   优点: 跳间可并行，复用 EdgeExpand 向量化实现
   缺点: 跳数可变时 pipeline 结构需动态构建
```

### 6.5 其他关键算子设计概要

**GroupBy (Sink)**：
```
Sink: 对每个 chunk 的 group key 计算 hash，插入 hash table，更新聚合状态
Finalize: 无需额外操作
Source: 遍历 hash table，按 chunk 输出 (group_key, agg_result)
```

**OrderBy (Sink)**：
```
Sink: 收集所有 chunk 到排序缓冲区
Finalize: 全量排序
Source: 按序分 chunk 输出
```

**Intersect (Sink)**：
```
Sink: 收集所有输入路径的邻居集合
Finalize: 多路求交
Source: 输出交集结果
```

**Join (Sink + Operator)**：
```
Build Side (Sink): 收集 build 侧数据，构建 hash table
Probe Side (Operator): 流式处理 probe 侧，逐 chunk 探测 hash table
```

---

## 7. Pipeline 构建与执行

### 7.1 Pipeline 数据结构

```cpp
class VecPipeline {
public:
    IPhysicalSource* source;
    vector<IPhysicalOperator*> operators;
    ISinkOperator* sink;

    // 依赖的上游 pipeline（必须在本 pipeline 之前完成）
    vector<VecPipeline*> dependencies;
};
```

### 7.2 从物理计划到 Pipeline DAG

```cpp
class PipelineBuilder {
public:
    // 输入: 物理算子树
    // 输出: pipeline DAG
    vector<unique_ptr<VecPipeline>> Build(PhysicalPlan& plan);

private:
    // 遍历物理算子树，在 pipeline breaker 处切断
    void BuildRecursive(PhysicalOperator& op, VecPipeline& current);
};
```

构建规则：
1. 自顶向下遍历物理计划树
2. 遇到 Sink 型算子（GroupBy, OrderBy, PathExpand, Join Build Side）→ 切断当前 pipeline，创建新 pipeline
3. Source 型算子成为 pipeline 的起点
4. Operator 型算子串联在 pipeline 中间

示例：

```
物理计划:
  Sink(Result)
    └── OrderBy
          └── Project
                └── EdgeExpand
                      └── Filter
                            └── Scan(Person)

生成的 Pipeline DAG:

Pipeline 1: Scan(Person) → Filter → EdgeExpand → Project → [OrderBy Sink]
Pipeline 2: [OrderBy Source] → [Result Sink]

依赖: Pipeline 2 依赖 Pipeline 1
```

### 7.3 Pipeline 执行器

```cpp
class PipelineExecutor {
public:
    void Execute(VecPipeline& pipeline, ExecutionContext& ctx) {
        // 初始化状态
        auto global_source_state = pipeline.source->GetGlobalSourceState();
        auto local_source_state = pipeline.source->GetLocalSourceState(*global_source_state);
        auto global_sink_state = pipeline.sink->GetGlobalSinkState();
        auto local_sink_state = pipeline.sink->GetLocalSinkState(*global_sink_state);

        vector<unique_ptr<OperatorState>> op_states;
        for (auto& op : pipeline.operators) {
            op_states.push_back(op->GetOperatorState());
        }

        GraphDataChunk source_chunk, intermediate;
        source_chunk.Initialize(source_types_);
        intermediate.Initialize(sink_types_);

        // 主循环
        while (true) {
            source_chunk.Reset();
            auto source_result = pipeline.source->GetData(
                ctx, source_chunk, *global_source_state, *local_source_state);

            if (source_chunk.size() == 0) break;

            // 通过算子链
            GraphDataChunk* current = &source_chunk;
            bool need_more_output = false;

            do {
                need_more_output = false;
                GraphDataChunk* input = current;

                for (size_t i = 0; i < pipeline.operators.size(); i++) {
                    intermediate.Reset();
                    auto result = pipeline.operators[i]->Execute(
                        ctx, *input, intermediate, *op_states[i]);
                    if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
                        need_more_output = true;
                    }
                    input = &intermediate;
                }

                if (intermediate.size() > 0) {
                    pipeline.sink->Sink(ctx, intermediate,
                                        *global_sink_state, *local_sink_state);
                }
            } while (need_more_output);

            if (source_result == SourceResultType::FINISHED) break;
        }

        // Finalize
        pipeline.sink->Combine(*global_sink_state, *local_sink_state);
        pipeline.sink->Finalize(*global_sink_state);
    }
};
```

### 7.4 DAG 调度器

```cpp
class DAGScheduler {
public:
    void Execute(vector<unique_ptr<VecPipeline>>& pipelines,
                 ExecutionContext& ctx) {
        // 拓扑排序
        auto sorted = TopologicalSort(pipelines);

        // 按依赖顺序执行（单线程版本）
        for (auto* pipeline : sorted) {
            PipelineExecutor executor;
            executor.Execute(*pipeline, ctx);
        }
    }
};
```

---

## 8. 存储层适配

### 8.1 现有存储接口

Neug 的 CSR 存储已经支持按顶点随机访问邻居：

```cpp
// 现有接口
auto view = graph.GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
auto edges = view.get_edges(vid);
for (auto it = edges.begin(); it != edges.end(); ++it) {
    vid_t nbr = it.get_vertex();
}
```

这对向量化是友好的 —— EdgeExpand 可以直接使用。

### 8.2 建议新增的批量接口

为了进一步提升向量化效率，建议增加批量邻居获取接口：

```cpp
class StorageReadInterface {
    // 批量获取多个顶点的邻居（提升 cache 友好性）
    virtual void GetNeighborsBatch(
        label_t src_label, label_t dst_label, label_t edge_label,
        const vid_t* vertices, idx_t count, Direction dir,
        // 输出：邻居数组 + 每个输入顶点的邻居偏移
        vector<vid_t>& neighbors,
        vector<idx_t>& offsets) = 0;

    // 批量获取顶点属性
    virtual void GetVertexPropertiesBatch(
        label_t label, const vid_t* vertices, idx_t count,
        int property_id, GraphVector& output) = 0;
};
```

---

## 9. 表达式求值向量化

### 9.1 现有表达式模型

当前表达式基于 `IContextColumn::get_elem(idx)` 逐行求值：

```cpp
// 现有: 逐行
Value result = expr->eval(ctx, row_idx);
```

### 9.2 向量化表达式求值

参考 DuckDB 的 `ExpressionExecutor`，改为批量求值：

```cpp
class VectorizedExpressionExecutor {
public:
    // 对一个 chunk 批量求值表达式
    void Execute(const Expression& expr,
                 const GraphDataChunk& input,
                 GraphVector& result);
};

// 示例：a.age > 30
// 1. 从 input chunk 中取出 "age" 列 → GraphVector age_vec
// 2. 创建常量 GraphVector const_30
// 3. 批量比较: GreaterThan(age_vec, const_30, result_vec)
//    内部循环: for (i = 0; i < count; i++) result[i] = age[i] > 30;
```

向量化表达式的优势：
- 循环体简单，编译器容易做 SIMD 自动向量化
- 减少虚函数调用（每个 chunk 调用一次，而非每行调用一次）
- Cache 友好：顺序访问连续内存

---

## 10. 实施路线

### Phase 1: 基础设施（预计 1-2 个月）

**目标**：建立核心数据结构，跑通最简单的 pipeline。

```
10.1  GraphVector 类型系统
      ├── VertexVector (label_t[] + vid_t[])
      ├── ValueVector<T> (typed flat array)
      ├── SelectionVector
      └── ValidityMask

10.2  GraphDataChunk
      ├── Initialize / Reset / SetCardinality
      ├── Slice (SelectionVector)
      └── Append

10.3  算子基类
      ├── IPhysicalSource + GlobalSourceState / LocalSourceState
      ├── IPhysicalOperator + OperatorState
      └── ISinkOperator + GlobalSinkState / LocalSinkState

10.4  最简 Pipeline 执行器
      └── Source → [Operators] → Sink 单线程循环

验收标准: Scan(Person) → Filter(age > 30) → Project(name) → ResultSink
         能够分 chunk 输出结果
```

### Phase 2: 关系型算子（预计 1-2 个月）

**目标**：补齐标准关系型算子。

```
10.5  Operator 类算子
      ├── FilterOperator (SelectionVector 过滤)
      ├── ProjectOperator (列投影 + 表达式)
      ├── LimitOperator (带早停)
      └── GetVOperator (边 → 顶点端点)

10.6  Sink 类算子
      ├── OrderBySink / OrderBySource
      ├── GroupBySink / GroupBySource (hash aggregate)
      ├── DedupSink / DedupSource
      └── HashJoinSink / HashJoinOperator

10.7  Pipeline 构建器
      └── PhysicalPlan → Pipeline DAG 转换

10.8  DAG 调度器
      └── 拓扑排序 + 顺序执行

验收标准: 能执行包含 Filter, Project, GroupBy, OrderBy 的查询
```

### Phase 3: 图算子向量化（预计 2-3 个月）

**目标**：实现图特有算子的向量化版本，这是整个重构最关键的阶段。

```
10.9  EdgeExpand (带状态的产出式算子)
      ├── expand_vertex 向量化版
      ├── expand_edge 向量化版
      ├── expand_degree / expand_count 向量化版
      └── 超级节点处理与测试

10.10 PathExpand v1 (Pipeline Breaker)
      ├── PathExpandSink (收集源顶点)
      ├── PathExpandFinalize (批量 BFS/DFS)
      ├── PathExpandSource (分 chunk 输出)
      └── 各变体: edge_expand_v, edge_expand_p, shortest_path

10.11 EdgeVector / PathVector 完善
      ├── EdgeVector 的完整读写操作
      └── PathVector 的变长存储布局

10.12 Intersect 算子
      └── 多路邻居交集的 Sink/Source 实现

验收标准: LDBC SNB Interactive 的核心查询能跑通
```

### Phase 4: 并行与优化（预计 2-3 个月）

**目标**：引入多线程并行执行，优化关键路径性能。

```
10.13 多线程 Pipeline 执行
      ├── Source 分区 (vertex range partitioning)
      ├── 多 PipelineExecutor 实例并发执行
      ├── LocalSinkState → Combine → GlobalSinkState
      └── 任务调度器

10.14 存储层批量接口
      ├── GetNeighborsBatch
      ├── GetVertexPropertiesBatch
      └── CSR prefetch 优化

10.15 表达式向量化
      ├── VectorizedExpressionExecutor
      ├── 算术 / 比较 / 逻辑表达式的批量求值
      └── SIMD 友好的循环结构

10.16 PathExpand 优化 (v2: Frontier-based)
      ├── 流式 frontier 展开
      ├── 按 chunk 输出满足条件的中间结果
      └── 内存控制与反压

验收标准: 多线程下性能线性扩展，LDBC SNB 基准测试全面通过
```

---

## 11. 风险与注意事项

### 11.1 EdgeExpand 是成败关键

EdgeExpand 在图查询中的地位类似关系查询中的 Table Scan —— 几乎每个查询都会用到，且往往是性能瓶颈。其向量化设计必须满足：

- **低延迟**：单次 `Execute` 调用的开销要小
- **超级节点友好**：一个顶点有 100 万邻居时不能 OOM
- **与 reshuffle 脱钩**：不再依赖全局 offset 数组重排

### 11.2 PathExpand 的 Pipeline Breaker 代价

PathExpand 作为 pipeline breaker 意味着前后要切断 pipeline。在路径查询密集的场景中，这会导致大量的 pipeline 切换和中间结果物化。v2 的 frontier-based 方案可以缓解，但实现复杂度显著上升。

### 11.3 渐进式迁移策略

建议不要一次性重写所有算子，而是：

1. 新旧引擎并存，通过配置切换
2. 先迁移 Scan → Filter → Project → Sink 路径
3. 逐步替换图算子
4. 用 LDBC SNB 查询做回归测试

### 11.4 STANDARD_VECTOR_SIZE 的选择

DuckDB 使用 2048，这个值在 CPU cache 和向量化效率之间取平衡。图查询可能需要实验不同值：

- 2048 行的 VertexVector = 2048 * (sizeof(label_t) + sizeof(vid_t)) ≈ 2048 * 12 = 24 KB
- 适合 L1 cache（通常 32-64 KB）
- EdgeExpand 展开后的行数可能远超 2048，但单个 chunk 不超过这个限制

### 11.5 与现有 Compiler/Planner 的衔接

现有的 `PlanParser`（`plan_parser.cc`）负责从 protobuf 物理计划构建 `IOperator` 链。重构后需要：

- 新增一个 `VecPlanParser`，从同一份 protobuf 构建新的 Source/Operator/Sink 算子
- 或者修改现有 `PlanParser`，增加向量化模式的分支
