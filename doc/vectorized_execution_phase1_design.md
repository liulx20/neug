# Phase 1: 向量化执行引擎基础设施 — 详细设计

## 1. 目标

建立向量化执行引擎的核心数据结构和最小可用的 pipeline 执行框架，跑通一条端到端的查询：

```
VertexScan(Person) → Select(age > 30) → Project(name) → ResultSink
```

Phase 1 完成后，新引擎与旧引擎并存，可通过配置切换。

---

## 2. 目录结构

新增代码全部放在 `execution/vectorized/` 目录下，与现有代码物理隔离：

```
include/neug/execution/vectorized/
├── core/
│   ├── validity_mask.h           // NULL 位图
│   ├── selection_vector.h        // 选择向量（零拷贝过滤）
│   ├── graph_vector.h            // 列向量基类 + 各类型特化
│   ├── graph_data_chunk.h        // 批量数据单元
│   └── constants.h               // STANDARD_VECTOR_SIZE 等常量
├── operators/
│   ├── physical_operator.h       // Source / Operator / Sink 三角色基类
│   ├── scan.h                    // VertexScanSource
│   ├── select.h                  // SelectOperator
│   ├── project.h                 // ProjectOperator
│   └── result_sink.h             // ResultSink
├── pipeline/
│   ├── vec_pipeline.h            // Pipeline 定义
│   └── pipeline_executor.h       // Pipeline 执行器
└── execution_context.h           // 执行上下文

src/execution/vectorized/
├── core/
│   ├── validity_mask.cc
│   ├── selection_vector.cc
│   ├── graph_vector.cc
│   └── graph_data_chunk.cc
├── operators/
│   ├── scan.cc
│   ├── select.cc
│   ├── project.cc
│   └── result_sink.cc
└── pipeline/
    ├── vec_pipeline.cc
    └── pipeline_executor.cc
```

---

## 3. 核心数据结构

### 3.1 常量定义

```cpp
// include/neug/execution/vectorized/core/constants.h
#pragma once
#include <cstddef>

namespace neug::execution::vec {

static constexpr size_t STANDARD_VECTOR_SIZE = 2048;

// SelectionVector 的索引类型，与 DuckDB 保持一致
// 索引范围为 [0, STANDARD_VECTOR_SIZE)，uint32_t 足够且省一半内存
using sel_t = uint32_t;

}  // namespace neug::execution::vec
```

选择 2048 的理由：
- VertexVector 一个 chunk = 2048 × (1 + 4) = 10 KB，远在 L1 cache 内
- DuckDB 经验验证过的默认值，在向量化效率与 cache 友好性之间的最佳平衡
- 后续可通过编译宏调整

---

### 3.2 ValidityMask

标记 chunk 中每一行是否为 NULL。设计为 64 位对齐的位图。

```cpp
// include/neug/execution/vectorized/core/validity_mask.h
#pragma once
#include <cstdint>
#include <cstring>
#include <memory>
#include "neug/execution/vectorized/core/constants.h"

namespace neug::execution::vec {

class ValidityMask {
 public:
  static constexpr size_t BITS_PER_ENTRY = 64;
  static constexpr size_t ENTRY_COUNT(size_t count) {
    return (count + BITS_PER_ENTRY - 1) / BITS_PER_ENTRY;
  }

  ValidityMask() : data_(nullptr), count_(0) {}

  explicit ValidityMask(size_t count) : count_(count) {
    size_t entries = ENTRY_COUNT(count);
    data_ = std::make_unique<uint64_t[]>(entries);
    // 全部标记为有效 (1 = valid, 0 = null)
    std::memset(data_.get(), 0xFF, entries * sizeof(uint64_t));
  }

  // 全部有效时 data_ 可为 nullptr（优化常见 case）
  bool AllValid() const { return data_ == nullptr; }

  void Initialize(size_t count) {
    count_ = count;
    size_t entries = ENTRY_COUNT(count);
    data_ = std::make_unique<uint64_t[]>(entries);
    std::memset(data_.get(), 0xFF, entries * sizeof(uint64_t));
  }

  // 将第 idx 行标记为 NULL
  void SetInvalid(size_t idx) {
    EnsureAllocated();
    size_t entry_idx = idx / BITS_PER_ENTRY;
    size_t bit_idx = idx % BITS_PER_ENTRY;
    data_[entry_idx] &= ~(uint64_t(1) << bit_idx);
  }

  // 将第 idx 行标记为有效
  void SetValid(size_t idx) {
    if (!data_) return;  // 已经全部有效
    size_t entry_idx = idx / BITS_PER_ENTRY;
    size_t bit_idx = idx % BITS_PER_ENTRY;
    data_[entry_idx] |= (uint64_t(1) << bit_idx);
  }

  bool IsValid(size_t idx) const {
    if (!data_) return true;
    size_t entry_idx = idx / BITS_PER_ENTRY;
    size_t bit_idx = idx % BITS_PER_ENTRY;
    return (data_[entry_idx] >> bit_idx) & 1;
  }

  // 重置为全部有效（释放位图内存）
  void Reset() { data_.reset(); }

  // 重置为全部有效（保留内存）
  void SetAllValid() {
    if (data_) {
      std::memset(data_.get(), 0xFF, ENTRY_COUNT(count_) * sizeof(uint64_t));
    }
  }

  size_t count() const { return count_; }

 private:
  void EnsureAllocated() {
    if (!data_) {
      Initialize(count_);
    }
  }

  std::unique_ptr<uint64_t[]> data_;
  size_t count_;
};

}  // namespace neug::execution::vec
```

设计说明：
- `data_ == nullptr` 表示全部有效，避免无 NULL 场景下的额外开销
- 64 位粒度，可用 `__builtin_popcountll` 快速统计
- 与 DuckDB 的 `ValidityMask` 语义一致

---

### 3.3 SelectionVector

用于零拷贝过滤。Filter 操作不移动数据，只产出一个 SelectionVector 标记哪些行通过。

```cpp
// include/neug/execution/vectorized/core/selection_vector.h
#pragma once
#include <cstdint>
#include <memory>
#include "neug/execution/vectorized/core/constants.h"

namespace neug::execution::vec {

class SelectionVector {
 public:
  SelectionVector() : sel_(nullptr), count_(0), owned_(false) {}

  // 创建一个 count 大小的 selection vector
  explicit SelectionVector(size_t count)
      : owned_data_(std::make_unique<sel_t[]>(count)),
        sel_(owned_data_.get()),
        count_(count),
        owned_(true) {}

  // 从外部数组创建（不拥有内存）
  SelectionVector(sel_t* data, size_t count)
      : sel_(data), count_(count), owned_(false) {}

  sel_t GetIndex(size_t i) const { return sel_[i]; }
  void SetIndex(size_t i, sel_t val) { sel_[i] = val; }

  sel_t* data() { return sel_; }
  const sel_t* data() const { return sel_; }
  size_t count() const { return count_; }

  // sel_ == nullptr 时退化为 identity（与 DuckDB 一致）
  // 避免为无过滤场景分配内存
  bool IsSet() const { return sel_ != nullptr; }

  // 初始化为 identity: [0, 1, 2, ..., count-1]
  void InitializeIdentity(size_t count) {
    if (!owned_ || count_ < count) {
      owned_data_ = std::make_unique<sel_t[]>(count);
      sel_ = owned_data_.get();
      owned_ = true;
    }
    count_ = count;
    for (size_t i = 0; i < count; i++) {
      sel_[i] = static_cast<sel_t>(i);
    }
  }

 private:
  std::unique_ptr<sel_t[]> owned_data_;
  sel_t* sel_;
  size_t count_;
  bool owned_;
};

}  // namespace neug::execution::vec
```

---

### 3.4 GraphVector 与 VectorBuffer 体系

列向量是整个系统最核心的数据结构。

#### 设计原则

参考 DuckDB 的架构，将 GraphVector 拆为两层：

- **GraphVector**（薄壳）：只持有 `DataType` + 指向 `VectorBuffer` 的指针
- **VectorBuffer**（多态子类）：各类型自行管理存储，互不干扰

```
GraphVector
├── type_: DataType
└── buffer_: unique_ptr<VectorBuffer>   ← 指向下面某个子类

VectorBuffer (抽象基类)
├── FlatVectorBuffer       → data[]: T[cap] + ValidityMask     (标量、Date、Interval 等)
├── StringVectorBuffer     → data[]: string_t[cap] + StringHeap + ValidityMask
├── StructVectorBuffer     → vector<GraphVector> children + ValidityMask  (Vertex、Edge、通用 Struct)
└── ListVectorBuffer       → data[]: list_entry_t[cap] + GraphVector child + ValidityMask  (Path、通用 List)
```

这样做的好处：
1. **每个实例只持有自己需要的成员**——int32 vector 不会为 children/list_child/string_heap 付开销
2. **图类型通过组合表达**——Vertex 就是 Struct(label, vid)，不需要特化存储逻辑
3. **可扩展**——后续加 DictionaryBuffer、ConstantBuffer 只需加子类，不改 GraphVector

#### VectorType（向量的逻辑解释方式）

与 `VectorBufferType`（物理存储方式）正交的一个维度：同一个 buffer 结构，不同的 `VectorType` 决定如何解释其中的数据。

```cpp
// include/neug/execution/vectorized/core/vector_type.h
#pragma once
#include <cstdint>

namespace neug::execution::vec {

enum class VectorType : uint8_t {
  kFlat,       // data[i] 是第 i 行的值（常规模式）
  kConstant,   // data[0] 代表所有行；buffer capacity = 1
};

}  // namespace neug::execution::vec
```

**为什么需要 ConstantVector？**

neug 的顶点列在绝大多数场景下是单 label 的（对应现有 `SLVertexColumn`）。如果 Vertex = Struct{label, vid}，
label 数组在单 label scan 后全是同一个值——2048 × 1 byte 的 data + 一条 cache line 全是重复的。

ConstantVector 用 1 个元素代表所有行，映射关系：

| 现有类型 | VectorType 表达 |
|----------|----------------|
| SLVertexColumn (单 label) | label child = **kConstant**，vid child = kFlat |
| MSVertexColumn (分段多 label) | VertexScan 按 label 分 chunk 发出，每个 chunk 的 label child = **kConstant** |
| MLVertexColumn (逐行多 label) | label child = kFlat，vid child = kFlat |

ConstantVector 不仅用于 label，任何"所有行同值"的场景都适用：常量表达式、聚合后的 group key、broadcast join key 等。

**使用模式：**

```cpp
auto& label_child = StructVector::GetChild(vertex_vec, 0);
if (label_child.IsConstant()) {
  // 快速路径：所有行同 label
  label_t label = label_child.GetData<label_t>()[0];
  // ... 用 label 处理所有行
} else {
  // 通用路径：逐行不同 label
  label_t* labels = label_child.GetData<label_t>();
  for (size_t i = 0; i < count; i++) {
    // ... 用 labels[i]
  }
}
```

如果算子不想处理两种模式，可以先调 `Flatten(vec, count)` 将 Constant 展开为 Flat：

```cpp
// 将 ConstantVector 展开为 FlatVector（复制 data[0] 到 data[0..count-1]）
// 如果已经是 Flat，则为 no-op
void Flatten(GraphVector& vec, size_t count);
```

---

#### VectorBuffer 基类

```cpp
// include/neug/execution/vectorized/core/vector_buffer.h
#pragma once
#include <cstdint>
#include <memory>
#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/validity_mask.h"

namespace neug::execution::vec {

class GraphVector;

enum class VectorBufferType : uint8_t {
  kFlat,       // 固定大小标量
  kString,     // 变长字符串
  kStruct,     // 结构体（含 Vertex、Edge）
  kList,       // 列表（含 Path）
};

class VectorBuffer {
 public:
  explicit VectorBuffer(VectorBufferType type, size_t capacity)
      : buffer_type_(type), capacity_(capacity) {}
  virtual ~VectorBuffer() = default;

  VectorBufferType buffer_type() const { return buffer_type_; }
  size_t capacity() const { return capacity_; }

  virtual ValidityMask& validity() = 0;
  virtual const ValidityMask& validity() const = 0;

  // data 指针（FlatBuffer / StringBuffer / ListBuffer 有，StructBuffer 返回 nullptr）
  virtual uint8_t* GetData() { return nullptr; }
  virtual const uint8_t* GetData() const { return nullptr; }

 protected:
  VectorBufferType buffer_type_;
  size_t capacity_;
};

}  // namespace neug::execution::vec
```

#### FlatVectorBuffer

存储固定大小标量类型（int32, int64, double, bool, Date, DateTime, Interval, ...）。

```cpp
// include/neug/execution/vectorized/core/flat_vector_buffer.h
#pragma once
#include "neug/execution/vectorized/core/vector_buffer.h"

namespace neug::execution::vec {

class FlatVectorBuffer : public VectorBuffer {
 public:
  FlatVectorBuffer(size_t type_size, size_t capacity)
      : VectorBuffer(VectorBufferType::kFlat, capacity),
        data_(std::make_unique<uint8_t[]>(type_size * capacity)),
        validity_(capacity) {}

  uint8_t* GetData() override { return data_.get(); }
  const uint8_t* GetData() const override { return data_.get(); }
  ValidityMask& validity() override { return validity_; }
  const ValidityMask& validity() const override { return validity_; }

 private:
  std::unique_ptr<uint8_t[]> data_;   // T[capacity]，按 sizeof(T) 解释
  ValidityMask validity_;
};

}  // namespace neug::execution::vec
```

#### StringVectorBuffer

String 的 data array 存放 `string_t`（16 字节固定大小 inline 结构），长字符串的实际数据存在 `StringHeap` 里。

```cpp
// include/neug/execution/vectorized/core/string_vector_buffer.h
#pragma once
#include "neug/execution/vectorized/core/vector_buffer.h"
#include <string>

namespace neug::execution::vec {

// 16 字节 inline string 结构（与 DuckDB string_t 相同思路）
struct string_t {
  uint32_t length;
  union {
    // 短字符串（<= 12 字节）直接内联
    char inlined[12];
    // 长字符串：前 4 字节做 prefix（用于快速比较），后 8 字节指向 heap
    struct {
      char prefix[4];
      const char* ptr;
    } pointer;
  };

  static constexpr size_t INLINE_LENGTH = 12;

  const char* GetData() const {
    return length <= INLINE_LENGTH ? inlined : pointer.ptr;
  }
  std::string GetString() const { return std::string(GetData(), length); }
};

// 简易 arena allocator，分配的内存在 StringHeap 析构时统一释放
class StringHeap {
 public:
  char* Allocate(size_t size);
  void Reset();
 private:
  std::vector<std::unique_ptr<char[]>> blocks_;
};

class StringVectorBuffer : public VectorBuffer {
 public:
  explicit StringVectorBuffer(size_t capacity)
      : VectorBuffer(VectorBufferType::kString, capacity),
        data_(std::make_unique<uint8_t[]>(sizeof(string_t) * capacity)),
        validity_(capacity) {}

  uint8_t* GetData() override { return data_.get(); }
  const uint8_t* GetData() const override { return data_.get(); }
  ValidityMask& validity() override { return validity_; }
  const ValidityMask& validity() const override { return validity_; }

  StringHeap& heap() { return heap_; }

  string_t* GetStringData() {
    return reinterpret_cast<string_t*>(data_.get());
  }

  // 添加字符串：短字符串 inline，长字符串拷入 heap
  string_t AddString(const char* data, size_t len);
  string_t AddString(const std::string& str) {
    return AddString(str.data(), str.size());
  }

 private:
  std::unique_ptr<uint8_t[]> data_;  // string_t[capacity]
  StringHeap heap_;                  // 长字符串溢出存储
  ValidityMask validity_;
};

}  // namespace neug::execution::vec
```

#### StructVectorBuffer

Struct 类型的数据**不在 data_ 中**，而是分散在 child vectors 里。

Vertex、Edge 都是 Struct 的特例：

```
Vertex = Struct { label: kUInt8, vid: kUInt32 }
Edge   = Struct { edge_label: kUInt8, src: kUInt32, dst: kUInt32, src_label: kUInt8, dst_label: kUInt8 }
```

```cpp
// include/neug/execution/vectorized/core/struct_vector_buffer.h
#pragma once
#include <vector>
#include "neug/execution/vectorized/core/vector_buffer.h"

namespace neug::execution::vec {

class GraphVector;  // 前向声明

class StructVectorBuffer : public VectorBuffer {
 public:
  // 用 child types 列表构造
  StructVectorBuffer(std::vector<std::unique_ptr<GraphVector>> children,
                     size_t capacity)
      : VectorBuffer(VectorBufferType::kStruct, capacity),
        children_(std::move(children)),
        validity_(capacity) {}

  ValidityMask& validity() override { return validity_; }
  const ValidityMask& validity() const override { return validity_; }

  size_t ChildCount() const { return children_.size(); }
  GraphVector& GetChild(size_t idx) { return *children_[idx]; }
  const GraphVector& GetChild(size_t idx) const { return *children_[idx]; }

  std::vector<std::unique_ptr<GraphVector>>& GetChildren() { return children_; }
  const std::vector<std::unique_ptr<GraphVector>>& GetChildren() const {
    return children_;
  }

 private:
  std::vector<std::unique_ptr<GraphVector>> children_;  // 通过指针持有，避免递归类型问题
  ValidityMask validity_;
};

}  // namespace neug::execution::vec
```

> **为什么 children 用 `unique_ptr<GraphVector>` 而不是直接存值？**
>
> `GraphVector` 包含 `unique_ptr<VectorBuffer>`，而 `StructVectorBuffer` 又包含 `GraphVector`——
> 这是一个递归类型。`std::vector<GraphVector>` 在技术上可行（vector 元素在堆上），
> 但用 `unique_ptr<GraphVector>` 更明确：
> 1. 避免移动 GraphVector 对象本身（只移动指针）
> 2. 语义清晰——children 是 buffer 拥有的子 vector，生命周期绑定

#### ListVectorBuffer

List 类型用 `list_entry_t` 数组（每个 entry 记录 offset 和 length）+ 一个 child vector（平铺存储所有元素）。

Path 是 List 的特例：`Path = List(PathNode)`。

```cpp
// include/neug/execution/vectorized/core/list_vector_buffer.h
#pragma once
#include "neug/execution/vectorized/core/vector_buffer.h"

namespace neug::execution::vec {

class GraphVector;  // 前向声明

struct list_entry_t {
  uint64_t offset;   // 在 child vector 中的起始位置
  uint64_t length;   // 元素个数
};

class ListVectorBuffer : public VectorBuffer {
 public:
  ListVectorBuffer(std::unique_ptr<GraphVector> child, size_t capacity)
      : VectorBuffer(VectorBufferType::kList, capacity),
        data_(std::make_unique<uint8_t[]>(sizeof(list_entry_t) * capacity)),
        child_(std::move(child)),
        validity_(capacity) {}

  uint8_t* GetData() override { return data_.get(); }
  const uint8_t* GetData() const override { return data_.get(); }
  ValidityMask& validity() override { return validity_; }
  const ValidityMask& validity() const override { return validity_; }

  list_entry_t* GetListEntries() {
    return reinterpret_cast<list_entry_t*>(data_.get());
  }
  const list_entry_t* GetListEntries() const {
    return reinterpret_cast<const list_entry_t*>(data_.get());
  }

  GraphVector& GetChild() { return *child_; }
  const GraphVector& GetChild() const { return *child_; }

 private:
  std::unique_ptr<uint8_t[]> data_;    // list_entry_t[capacity]
  std::unique_ptr<GraphVector> child_; // 所有 list 元素平铺存储
  ValidityMask validity_;
};

}  // namespace neug::execution::vec
```

#### GraphVector（薄壳）

GraphVector 本身只持有类型和 buffer 指针，所有访问通过 buffer 间接完成：

```cpp
// include/neug/execution/vectorized/core/graph_vector.h
#pragma once
#include "neug/common/types.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/vectorized/core/vector_buffer.h"
#include "neug/execution/vectorized/core/flat_vector_buffer.h"
#include "neug/execution/vectorized/core/string_vector_buffer.h"
#include "neug/execution/vectorized/core/struct_vector_buffer.h"
#include "neug/execution/vectorized/core/list_vector_buffer.h"

namespace neug::execution::vec {

// 返回 DataTypeId 对应的固定大小标量字节数，非标量返回 0
size_t GetTypeSize(DataTypeId type_id);

class GraphVector {
 public:
  GraphVector();
  explicit GraphVector(DataType type, size_t capacity = STANDARD_VECTOR_SIZE);
  ~GraphVector();

  GraphVector(GraphVector&& other) noexcept;
  GraphVector& operator=(GraphVector&& other) noexcept;
  GraphVector(const GraphVector&) = delete;
  GraphVector& operator=(const GraphVector&) = delete;

  void Initialize(DataType type, size_t capacity = STANDARD_VECTOR_SIZE);

  const DataType& type() const { return type_; }
  DataTypeId type_id() const { return type_.id(); }
  VectorType vector_type() const { return vector_type_; }
  bool IsConstant() const { return vector_type_ == VectorType::kConstant; }
  bool IsFlat() const { return vector_type_ == VectorType::kFlat; }

  VectorBuffer& buffer() { return *buffer_; }
  const VectorBuffer& buffer() const { return *buffer_; }
  VectorBufferType buffer_type() const { return buffer_->buffer_type(); }

  ValidityMask& validity() { return buffer_->validity(); }
  const ValidityMask& validity() const { return buffer_->validity(); }

  // ─── 标量快速访问（FlatVectorBuffer 专用）───

  template <typename T>
  T* GetData() {
    return reinterpret_cast<T*>(buffer_->GetData());
  }

  template <typename T>
  const T* GetData() const {
    return reinterpret_cast<const T*>(buffer_->GetData());
  }

  // ─── 通用 Value 读写（慢路径，所有类型通用）───

  void SetGenericValue(size_t idx, const execution::Value& val);
  execution::Value GetGenericValue(size_t idx) const;

  // ─── Constant / Flat 转换 ───

  void SetVectorType(VectorType vtype) { vector_type_ = vtype; }

 private:
  DataType type_;
  VectorType vector_type_ = VectorType::kFlat;
  std::unique_ptr<VectorBuffer> buffer_;
};

// 将 ConstantVector 展开为 FlatVector（复制 data[0] 到 data[0..count-1]）
// 对 Struct 类型会递归展开所有 children
// 已经是 Flat 则为 no-op
void Flatten(GraphVector& vec, size_t count);

// ─── 辅助函数（类型安全的访问器，避免使用者直接 cast buffer）───

namespace StructVector {
  GraphVector& GetChild(GraphVector& vec, size_t idx);
  const GraphVector& GetChild(const GraphVector& vec, size_t idx);
  size_t GetChildCount(const GraphVector& vec);
}

namespace ListVector {
  GraphVector& GetChild(GraphVector& vec);
  const GraphVector& GetChild(const GraphVector& vec);
  list_entry_t* GetEntries(GraphVector& vec);
  const list_entry_t* GetEntries(const GraphVector& vec);
}

namespace StringVector {
  string_t* GetStringData(GraphVector& vec);
  string_t AddString(GraphVector& vec, const std::string& str);
}

// ─── 图类型便捷访问器 ───

namespace VertexVector {
  // Vertex = Struct { children[0]: label_t (kUInt8), children[1]: vid_t (kUInt32) }
  label_t* GetLabels(GraphVector& vec);
  vid_t* GetVids(GraphVector& vec);
  const label_t* GetLabels(const GraphVector& vec);
  const vid_t* GetVids(const GraphVector& vec);

  void SetVertex(GraphVector& vec, size_t idx, label_t label, vid_t vid);
  VertexRecord GetVertex(const GraphVector& vec, size_t idx);

  // label child 是否为 Constant（对应旧 SLVertexColumn 语义）
  bool IsConstantLabel(const GraphVector& vec);
  label_t GetConstantLabel(const GraphVector& vec);

  // 创建一个 Vertex 类型的 GraphVector（label child = Flat, vid child = Flat）
  GraphVector Create(size_t capacity = STANDARD_VECTOR_SIZE);

  // 创建单 label Vertex（label child = Constant, vid child = Flat）
  // 对应 SLVertexColumn：所有顶点共享同一个 label
  GraphVector CreateSingleLabel(label_t label, size_t capacity = STANDARD_VECTOR_SIZE);
}

namespace EdgeVector {
  // Edge = Struct { edge_label, src_vid, dst_vid, src_label, dst_label }
  void SetEdge(GraphVector& vec, size_t idx, const EdgeRecord& edge);
  EdgeRecord GetEdge(const GraphVector& vec, size_t idx);

  GraphVector Create(size_t capacity = STANDARD_VECTOR_SIZE);
}

}  // namespace neug::execution::vec
```

#### 各类型的内存布局详解

**标量类型** (int32, int64, double, bool, Date, DateTime, Interval, uint32, uint64, float)：

```
GraphVector (type = kInt64)
└── FlatVectorBuffer
    ├── data_: int64_t[2048]           连续 flat array
    └── validity_: ValidityMask

    内存: 2048 × 8 = 16 KB
```

**String 类型**：

```
GraphVector (type = kVarchar)
└── StringVectorBuffer
    ├── data_: string_t[2048]          每个 16 字节，固定大小
    ├── heap_: StringHeap              长字符串（> 12 字节）的溢出存储
    └── validity_: ValidityMask

    string_t 内部:
    ┌──────────────────────────────────────┐
    │ length (4B) │ inline data (12B)      │  短字符串，全部内联
    │ length (4B) │ prefix (4B) │ ptr (8B) │  长字符串，ptr 指向 heap
    └──────────────────────────────────────┘

    内存: 2048 × 16 = 32 KB (data array) + heap 按需增长
```

**Vertex 类型 — 单 label（最常见，对应旧 SLVertexColumn）**：

```
GraphVector (type = kVertex, vector_type = kFlat)
└── StructVectorBuffer
    ├── children_[0]: GraphVector (kUInt8, vector_type = kConstant)
    │   └── FlatVectorBuffer (capacity = 1)
    │       └── data_: label_t[1] = {person_label}     1 byte
    ├── children_[1]: GraphVector (kUInt32, vector_type = kFlat)
    │   └── FlatVectorBuffer
    │       └── data_: vid_t[2048]                     4 bytes × 2048 = 8 KB
    └── validity_: ValidityMask

    总内存: ≈ 8 KB（省了 2 KB label 数组 + cache 更友好）
    创建: VertexVector::CreateSingleLabel(person_label)
    访问: VertexVector::GetConstantLabel(vec) 或 VertexVector::GetVids(vec)[i]
```

**Vertex 类型 — 多 label（对应旧 MLVertexColumn）**：

```
GraphVector (type = kVertex, vector_type = kFlat)
└── StructVectorBuffer
    ├── children_[0]: GraphVector (kUInt8, vector_type = kFlat)
    │   └── FlatVectorBuffer
    │       └── data_: label_t[2048]       1 byte × 2048 = 2 KB
    ├── children_[1]: GraphVector (kUInt32, vector_type = kFlat)
    │   └── FlatVectorBuffer
    │       └── data_: vid_t[2048]         4 bytes × 2048 = 8 KB
    └── validity_: ValidityMask

    总内存: 10 KB
    创建: VertexVector::Create()
    访问: VertexVector::GetLabels(vec)[i], VertexVector::GetVids(vec)[i]
```

**Edge 类型**（Struct 特例，5 个 child）：

```
GraphVector (type = kEdge)
└── StructVectorBuffer
    ├── children_[0]: GraphVector (kUInt8)   → edge_label[2048]    2 KB
    ├── children_[1]: GraphVector (kUInt32)  → src_vid[2048]       8 KB
    ├── children_[2]: GraphVector (kUInt32)  → dst_vid[2048]       8 KB
    ├── children_[3]: GraphVector (kUInt8)   → src_label[2048]     2 KB
    ├── children_[4]: GraphVector (kUInt8)   → dst_label[2048]     2 KB
    └── validity_: ValidityMask

    总内存: 22 KB
```

**Path 类型**（List 特例，Phase 3 实现）：

```
GraphVector (type = kPath)
└── ListVectorBuffer
    ├── data_: list_entry_t[2048]       每条路径的 {offset, length}
    ├── child_: GraphVector (kPathNode) 所有路径节点平铺
    │   └── StructVectorBuffer
    │       ├── children_[0]: label_t[]   节点 label
    │       ├── children_[1]: vid_t[]     节点 vid
    │       └── ...                       边信息
    └── validity_: ValidityMask

    例: chunk 中有 2048 条路径，平均长度 5 个节点
        → list_entry_t[2048] + PathNode[~10240]
```

**对比旧设计**：

| | 旧设计（一体式 GraphVector） | 新设计（GraphVector + VectorBuffer + VectorType） |
|---|---|---|
| int32 vector 的内存 | data_ + 空的 children_ + 空的 list_child_ + 空的 string_heap_ + validity_ | FlatVectorBuffer 中只有 data_ + validity_ |
| 单 label Vertex | 硬编码的 SoA 布局，label[2048] 全是同一个值（2 KB 浪费） | label child = ConstantVector（1 字节），仅 vid 占 8 KB |
| 多 label Vertex | 需要不同的 MLVertexColumn 类 | 同一个 Struct，label child 改为 kFlat 即可 |
| SL/MS/ML 三种列类型 | 3 个 class + dynamic_cast 分发 | 统一为 Vertex Struct，用 VectorType 区分 |
| String 存储 | `vector<string>`，每个 string 独立堆分配 | `string_t[2048]` 连续数组 + StringHeap arena |
| 常量表达式/广播值 | 无优化，需要逐行填充 | ConstantVector，1 个值代表所有行 |
| 新增类型的成本 | 改 GraphVector 加成员 + 加方法 | 加 VectorBuffer 子类，GraphVector 不变 |

---

### 3.5 GraphDataChunk

算子间流动的数据批次。

```cpp
// include/neug/execution/vectorized/core/graph_data_chunk.h
#pragma once
#include <vector>
#include "neug/common/types.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/core/selection_vector.h"

namespace neug::execution::vec {

class GraphDataChunk {
 public:
  GraphDataChunk() : count_(0) {}
  ~GraphDataChunk() = default;

  GraphDataChunk(GraphDataChunk&& other) noexcept = default;
  GraphDataChunk& operator=(GraphDataChunk&& other) noexcept = default;

  // ─── 初始化 ───

  // 按 (tag, type) 对初始化
  void Initialize(const std::vector<std::pair<int, DataType>>& columns,
                  size_t capacity = STANDARD_VECTOR_SIZE);

  // 重置为 0 行（保留内存分配）
  void Reset();

  // ─── 访问 ───

  size_t size() const { return count_; }
  size_t ColumnCount() const { return vectors_.size(); }
  void SetCardinality(size_t count) { count_ = count; }

  GraphVector& GetVector(size_t col_idx) { return vectors_[col_idx]; }
  const GraphVector& GetVector(size_t col_idx) const { return vectors_[col_idx]; }

  int GetTag(size_t col_idx) const { return tags_[col_idx]; }

  // 按 tag 查找列（返回列索引，找不到返回 -1）
  int FindColumnByTag(int tag) const;

  // 按 tag 查找并获取 vector 引用
  GraphVector& GetVectorByTag(int tag);
  const GraphVector& GetVectorByTag(int tag) const;

  // ─── schema 信息 ───

  const std::vector<int>& tags() const { return tags_; }
  std::vector<DataType> GetTypes() const;

  // ─── 数据操作 ───

  // 通过 SelectionVector 做零拷贝过滤
  // 物理移动数据到紧凑布局（就地压缩）
  void Compact(const SelectionVector& sel, size_t sel_count);

  // 追加另一个 chunk 的数据
  void Append(const GraphDataChunk& other, size_t count);

  // 增加一个列
  void AddColumn(int tag, DataType type, size_t capacity = STANDARD_VECTOR_SIZE);

  // ─── 调试 ───

  std::string ToString() const;

 private:
  std::vector<GraphVector> vectors_;
  std::vector<int> tags_;       // 每列的 tag/alias（图查询中列按 tag 引用）
  size_t count_;                // 当前有效行数
};

}  // namespace neug::execution::vec
```

设计说明：

- **tag 索引**：图查询中列不是按位置引用的（不是 `column[0]`, `column[1]`），而是按 `tag`/`alias` 引用（如 `tag=0` 是 Person 顶点，`tag=1` 是 knows 边）。这与关系型的 `column_index` 模式不同，是 neug 必须保留的语义。
- **Compact vs Slice**：DuckDB 用 Dictionary Vector 做零拷贝 Slice，但 Phase 1 先用物理移动（Compact），实现更简单。Phase 2 可以引入 Dictionary 模式。
- **动态加列**：EdgeExpand 等算子会在 chunk 中新增列（展开后的邻居列），`AddColumn` 支持这个场景。

---

## 4. 算子接口

### 4.1 ExecutionContext

```cpp
// include/neug/execution/vectorized/execution_context.h
#pragma once
#include "neug/execution/common/params_map.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug::execution::vec {

class ExecutionContext {
 public:
  ExecutionContext(const StorageReadInterface& graph, const ParamsMap& params)
      : graph_(graph), params_(params) {}

  const StorageReadInterface& graph() const { return graph_; }
  const ParamsMap& params() const { return params_; }

 private:
  const StorageReadInterface& graph_;
  const ParamsMap& params_;
};

}  // namespace neug::execution::vec
```

### 4.2 算子状态基类

```cpp
// include/neug/execution/vectorized/operators/physical_operator.h
#pragma once
#include <memory>
#include <string>
#include <vector>
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/execution_context.h"
#include "neug/utils/result.h"

namespace neug::execution::vec {

// ─── 状态基类 ───

struct SourceState {
  virtual ~SourceState() = default;
};

struct OperatorState {
  virtual ~OperatorState() = default;
};

struct SinkState {
  virtual ~SinkState() = default;
};

// ─── 返回值 ───

enum class SourceResultType {
  kHaveMoreOutput,
  kFinished,
};

enum class OperatorResultType {
  kNeedMoreInput,
  kHaveMoreOutput,
  kFinished,
};

enum class SinkResultType {
  kNeedMoreInput,
  kFinished,
};

// ─── Source 基类 ───

class IVecSource {
 public:
  virtual ~IVecSource() = default;
  virtual std::string GetName() const = 0;

  // 返回输出 chunk 的 schema: [(tag, type), ...]
  virtual std::vector<std::pair<int, DataType>> GetOutputSchema() const = 0;

  virtual std::unique_ptr<SourceState> InitState(
      const ExecutionContext& ctx) const = 0;

  virtual SourceResultType GetData(
      const ExecutionContext& ctx,
      GraphDataChunk& chunk,
      SourceState& state) const = 0;
};

// ─── Operator 基类 ───

class IVecOperator {
 public:
  virtual ~IVecOperator() = default;
  virtual std::string GetName() const = 0;

  virtual std::unique_ptr<OperatorState> InitState(
      const ExecutionContext& ctx) const = 0;

  // 返回值:
  //   kNeedMoreInput  - 当前 input 已处理完毕，需要新的 input
  //   kHaveMoreOutput - 当前 input 未处理完毕，需要再次调用（不传新 input）
  virtual OperatorResultType Execute(
      const ExecutionContext& ctx,
      GraphDataChunk& input,
      GraphDataChunk& output,
      OperatorState& state) const = 0;
};

// ─── Sink 基类 ───

class IVecSink {
 public:
  virtual ~IVecSink() = default;
  virtual std::string GetName() const = 0;

  virtual std::unique_ptr<SinkState> InitState(
      const ExecutionContext& ctx) const = 0;

  virtual SinkResultType Sink(
      const ExecutionContext& ctx,
      GraphDataChunk& chunk,
      SinkState& state) const = 0;

  // 所有数据 Sink 完成后调用
  virtual void Finalize(const ExecutionContext& ctx, SinkState& state) const = 0;
};

}  // namespace neug::execution::vec
```

### 4.3 关于 HAVE_MORE_OUTPUT 的说明

`IVecOperator::Execute` 返回 `kHaveMoreOutput` 时，表示**当前 input 尚未处理完毕**。调用方应继续调用 `Execute` 并传入**同一个 input**（不要 Reset），直到返回 `kNeedMoreInput`。

这是为 Phase 3 的 EdgeExpand 预留的：一个输入顶点可能展开出超过 STANDARD_VECTOR_SIZE 个邻居，需要多次调用才能输出完毕。

Phase 1 的简单算子（Select、Project）始终返回 `kNeedMoreInput`。

---

## 5. 具体算子实现

### 5.1 VertexScanSource

```cpp
// include/neug/execution/vectorized/operators/scan.h
#pragma once
#include "neug/execution/vectorized/operators/physical_operator.h"
#include "neug/execution/utils/params.h"

namespace neug::execution::vec {

struct VertexScanState : public SourceState {
  size_t current_label_idx = 0;   // 当前扫描到 tables 的第几个 label
  vid_t current_vid = 0;          // 当前 label 下扫描到的 vid
  bool finished = false;
};

class VertexScanSource : public IVecSource {
 public:
  explicit VertexScanSource(const execution::ScanParams& params)
      : params_(params) {}

  std::string GetName() const override { return "VertexScan"; }

  std::vector<std::pair<int, DataType>> GetOutputSchema() const override {
    return {{params_.alias, DataType(DataTypeId::kVertex)}};
  }

  std::unique_ptr<SourceState> InitState(
      const ExecutionContext& ctx) const override {
    return std::make_unique<VertexScanState>();
  }

  SourceResultType GetData(
      const ExecutionContext& ctx,
      GraphDataChunk& chunk,
      SourceState& state) const override;

 private:
  execution::ScanParams params_;
};

}  // namespace neug::execution::vec
```

实现核心逻辑：

VertexScan 按 label 逐个扫描。每个 label 内的所有 chunk 都使用 ConstantVector 存储 label（对应旧 SLVertexColumn 语义），只有在一个 chunk 内跨了 label 边界时才退化为 Flat。

```cpp
// src/execution/vectorized/operators/scan.cc
SourceResultType VertexScanSource::GetData(
    const ExecutionContext& ctx,
    GraphDataChunk& chunk,
    SourceState& state_base) const {
  auto& state = static_cast<VertexScanState&>(state_base);
  if (state.finished) return SourceResultType::kFinished;

  auto& vertex_vec = chunk.GetVectorByTag(params_.alias);
  vid_t* vids = VertexVector::GetVids(vertex_vec);
  size_t count = 0;

  // 记录当前 chunk 起始时的 label，用于判断是否 constant
  label_t start_label = params_.tables[state.current_label_idx];
  bool single_label = true;

  while (count < STANDARD_VECTOR_SIZE &&
         state.current_label_idx < params_.tables.size()) {
    label_t label = params_.tables[state.current_label_idx];
    if (label != start_label) {
      single_label = false;
    }
    auto vertex_set = ctx.graph().GetVertexSet(label);

    vid_t max_vid = vertex_set.size();
    while (count < STANDARD_VECTOR_SIZE && state.current_vid < max_vid) {
      if (vertex_set.contains(state.current_vid)) {
        if (!single_label) {
          // 多 label 场景：逐行写 label（需要先 Flatten label child）
          VertexVector::GetLabels(vertex_vec)[count] = label;
        }
        vids[count] = state.current_vid;
        count++;
      }
      state.current_vid++;
    }

    if (state.current_vid >= max_vid) {
      state.current_label_idx++;
      state.current_vid = 0;
    }
  }

  // 设置 label child 的 VectorType
  auto& label_child = StructVector::GetChild(vertex_vec, 0);
  if (single_label) {
    // 单 label：设为 Constant，只存一个值
    label_child.SetVectorType(VectorType::kConstant);
    label_child.GetData<label_t>()[0] = start_label;
  } else {
    // 多 label：已经逐行写入，确保是 Flat
    label_child.SetVectorType(VectorType::kFlat);
  }

  if (state.current_label_idx >= params_.tables.size()) {
    state.finished = true;
  }

  chunk.SetCardinality(count);
  return count > 0 ? SourceResultType::kHaveMoreOutput
                   : SourceResultType::kFinished;
}
```

### 5.2 SelectOperator

Filter / Select：基于谓词过滤行。Phase 1 先实现简单的按行求值过滤，Phase 4 再改为向量化表达式。

```cpp
// include/neug/execution/vectorized/operators/select.h
#pragma once
#include <functional>
#include "neug/execution/vectorized/operators/physical_operator.h"

namespace neug::execution::vec {

// 谓词函数签名：给定 chunk 和行号，返回是否通过
using ChunkPredicate = std::function<bool(const GraphDataChunk& chunk, size_t row)>;

struct SelectState : public OperatorState {};

class SelectOperator : public IVecOperator {
 public:
  explicit SelectOperator(ChunkPredicate predicate)
      : predicate_(std::move(predicate)) {}

  std::string GetName() const override { return "Select"; }

  std::unique_ptr<OperatorState> InitState(
      const ExecutionContext& ctx) const override {
    return std::make_unique<SelectState>();
  }

  OperatorResultType Execute(
      const ExecutionContext& ctx,
      GraphDataChunk& input,
      GraphDataChunk& output,
      OperatorState& state) const override;

 private:
  ChunkPredicate predicate_;
};

}  // namespace neug::execution::vec
```

实现：

```cpp
// src/execution/vectorized/operators/select.cc
OperatorResultType SelectOperator::Execute(
    const ExecutionContext& ctx,
    GraphDataChunk& input,
    GraphDataChunk& output,
    OperatorState& state) const {
  // 构建 selection vector
  SelectionVector sel(input.size());
  size_t sel_count = 0;

  for (size_t i = 0; i < input.size(); i++) {
    if (predicate_(input, i)) {
      sel.SetIndex(sel_count++, static_cast<sel_t>(i));
    }
  }

  if (sel_count == input.size()) {
    // 全部通过，直接移动
    output = std::move(input);
  } else if (sel_count > 0) {
    // 部分通过，compact
    input.Compact(sel, sel_count);
    output = std::move(input);
  } else {
    // 全部被过滤
    output.SetCardinality(0);
  }

  return OperatorResultType::kNeedMoreInput;
}
```

### 5.3 ProjectOperator

投影：选择或计算指定列，输出新 chunk。

```cpp
// include/neug/execution/vectorized/operators/project.h
#pragma once
#include <functional>
#include <vector>
#include "neug/execution/vectorized/operators/physical_operator.h"

namespace neug::execution::vec {

// 投影表达式：从 input chunk 计算一个输出列
struct VecProjectExpr {
  int output_tag;
  DataType output_type;

  // 求值函数：input chunk → 填充 output vector
  // count 是当前有效行数
  std::function<void(const GraphDataChunk& input,
                     GraphVector& output, size_t count)> evaluate;
};

struct ProjectState : public OperatorState {};

class ProjectOperator : public IVecOperator {
 public:
  explicit ProjectOperator(std::vector<VecProjectExpr> expressions)
      : expressions_(std::move(expressions)) {}

  std::string GetName() const override { return "Project"; }

  std::unique_ptr<OperatorState> InitState(
      const ExecutionContext& ctx) const override {
    return std::make_unique<ProjectState>();
  }

  OperatorResultType Execute(
      const ExecutionContext& ctx,
      GraphDataChunk& input,
      GraphDataChunk& output,
      OperatorState& state) const override;

 private:
  std::vector<VecProjectExpr> expressions_;
};

}  // namespace neug::execution::vec
```

实现：

```cpp
// src/execution/vectorized/operators/project.cc
OperatorResultType ProjectOperator::Execute(
    const ExecutionContext& ctx,
    GraphDataChunk& input,
    GraphDataChunk& output,
    OperatorState& state) const {

  // 构建 output schema 并初始化
  if (output.ColumnCount() == 0) {
    for (const auto& expr : expressions_) {
      output.AddColumn(expr.output_tag, expr.output_type);
    }
  }

  size_t count = input.size();
  for (size_t i = 0; i < expressions_.size(); i++) {
    auto& expr = expressions_[i];
    auto& out_vec = output.GetVectorByTag(expr.output_tag);
    expr.evaluate(input, out_vec, count);
  }

  output.SetCardinality(count);
  return OperatorResultType::kNeedMoreInput;
}
```

常用投影表达式工厂：

```cpp
namespace neug::execution::vec {

// 直接传递列（tag 不变）
VecProjectExpr MakePassthrough(int tag, DataType type);

// 顶点属性访问: 从 VertexVector 中取属性列
VecProjectExpr MakeVertexPropertyExpr(
    int input_vertex_tag, int output_tag, DataType output_type,
    label_t vertex_label, int property_id);

// 常量列
template <typename T>
VecProjectExpr MakeConstantExpr(int output_tag, DataType type, T value);

}  // namespace neug::execution::vec
```

### 5.4 ResultSink

收集所有输出 chunk，Phase 1 用最简单的方式把结果序列化为行式输出。

```cpp
// include/neug/execution/vectorized/operators/result_sink.h
#pragma once
#include "neug/execution/vectorized/operators/physical_operator.h"
#include "neug/execution/common/types/value.h"

namespace neug::execution::vec {

struct ResultSinkState : public SinkState {
  // 收集结果（简单版：逐行收集 Value）
  std::vector<std::vector<execution::Value>> rows;
  std::vector<int> output_tags;
};

class ResultSink : public IVecSink {
 public:
  explicit ResultSink(std::vector<int> output_tags)
      : output_tags_(std::move(output_tags)) {}

  std::string GetName() const override { return "ResultSink"; }

  std::unique_ptr<SinkState> InitState(
      const ExecutionContext& ctx) const override;

  SinkResultType Sink(
      const ExecutionContext& ctx,
      GraphDataChunk& chunk,
      SinkState& state) const override;

  void Finalize(const ExecutionContext& ctx, SinkState& state) const override {}

  // 获取结果（在 Finalize 后调用）
  static const std::vector<std::vector<execution::Value>>& GetResults(
      const SinkState& state);

 private:
  std::vector<int> output_tags_;
};

}  // namespace neug::execution::vec
```

实现：

```cpp
// src/execution/vectorized/operators/result_sink.cc
SinkResultType ResultSink::Sink(
    const ExecutionContext& ctx,
    GraphDataChunk& chunk,
    SinkState& state_base) const {
  auto& state = static_cast<ResultSinkState&>(state_base);

  for (size_t row = 0; row < chunk.size(); row++) {
    std::vector<execution::Value> row_values;
    for (int tag : output_tags_) {
      auto& vec = chunk.GetVectorByTag(tag);
      row_values.push_back(vec.GetGenericValue(row));
    }
    state.rows.push_back(std::move(row_values));
  }

  return SinkResultType::kNeedMoreInput;
}
```

---

## 6. Pipeline 定义与执行

### 6.1 VecPipeline

```cpp
// include/neug/execution/vectorized/pipeline/vec_pipeline.h
#pragma once
#include <memory>
#include <vector>
#include "neug/execution/vectorized/operators/physical_operator.h"

namespace neug::execution::vec {

class VecPipeline {
 public:
  VecPipeline() = default;

  void SetSource(std::unique_ptr<IVecSource> source) {
    source_ = std::move(source);
  }

  void AddOperator(std::unique_ptr<IVecOperator> op) {
    operators_.push_back(std::move(op));
  }

  void SetSink(std::unique_ptr<IVecSink> sink) {
    sink_ = std::move(sink);
  }

  IVecSource* source() const { return source_.get(); }
  const std::vector<std::unique_ptr<IVecOperator>>& operators() const {
    return operators_;
  }
  IVecSink* sink() const { return sink_.get(); }

 private:
  std::unique_ptr<IVecSource> source_;
  std::vector<std::unique_ptr<IVecOperator>> operators_;
  std::unique_ptr<IVecSink> sink_;
};

}  // namespace neug::execution::vec
```

### 6.2 PipelineExecutor

```cpp
// include/neug/execution/vectorized/pipeline/pipeline_executor.h
#pragma once
#include "neug/execution/vectorized/pipeline/vec_pipeline.h"
#include "neug/utils/result.h"

namespace neug::execution::vec {

class PipelineExecutor {
 public:
  // 执行单条 pipeline
  static neug::result<void> Execute(const VecPipeline& pipeline,
                                    const ExecutionContext& ctx);
};

}  // namespace neug::execution::vec
```

实现（这是 Phase 1 的核心执行循环）：

```cpp
// src/execution/vectorized/pipeline/pipeline_executor.cc
#include "neug/execution/vectorized/pipeline/pipeline_executor.h"

namespace neug::execution::vec {

neug::result<void> PipelineExecutor::Execute(
    const VecPipeline& pipeline,
    const ExecutionContext& ctx) {

  // 1. 初始化所有状态
  auto source_state = pipeline.source()->InitState(ctx);
  auto sink_state = pipeline.sink()->InitState(ctx);

  std::vector<std::unique_ptr<OperatorState>> op_states;
  for (auto& op : pipeline.operators()) {
    op_states.push_back(op->InitState(ctx));
  }

  // 2. 准备 chunk
  //    source_chunk: Source 输出
  //    inter_chunks: 算子链中间使用（需要两个交替）
  auto output_schema = pipeline.source()->GetOutputSchema();

  GraphDataChunk source_chunk;
  source_chunk.Initialize(output_schema);

  GraphDataChunk inter_a, inter_b;
  // inter chunk 的 schema 在算子链执行过程中动态确定

  // 3. 主循环
  while (true) {
    // 3.1 从 Source 获取数据
    source_chunk.Reset();
    auto source_result = pipeline.source()->GetData(ctx, source_chunk, *source_state);

    if (source_chunk.size() == 0 && source_result == SourceResultType::kFinished) {
      break;
    }
    if (source_chunk.size() == 0) {
      continue;
    }

    // 3.2 通过算子链
    GraphDataChunk* current_input = &source_chunk;
    bool have_more_output = false;

    do {
      have_more_output = false;
      GraphDataChunk* input = current_input;

      for (size_t i = 0; i < pipeline.operators().size(); i++) {
        auto& op = pipeline.operators()[i];
        GraphDataChunk op_output;
        // op_output 的 schema 由算子内部根据 input 决定

        auto op_result = op->Execute(ctx, *input, op_output, *op_states[i]);

        if (op_result == OperatorResultType::kHaveMoreOutput) {
          have_more_output = true;
        }

        // 算子的输出作为下一个算子的输入
        if (i == 0) {
          inter_a = std::move(op_output);
          input = &inter_a;
        } else {
          inter_b = std::move(op_output);
          input = &inter_b;
          std::swap(inter_a, inter_b);
          input = &inter_a;
        }
      }

      // 3.3 如果算子链为空，直接把 source output 送入 sink
      GraphDataChunk* final_output = pipeline.operators().empty()
                                         ? current_input
                                         : &inter_a;

      if (final_output->size() > 0) {
        pipeline.sink()->Sink(ctx, *final_output, *sink_state);
      }

    } while (have_more_output);

    if (source_result == SourceResultType::kFinished) {
      break;
    }
  }

  // 4. Finalize
  pipeline.sink()->Finalize(ctx, *sink_state);

  return {};
}

}  // namespace neug::execution::vec
```

---

## 7. 端到端示例

组装一条完整的查询 pipeline：`Scan(Person) → Select(age > 30) → Project(name) → ResultSink`

```cpp
#include "neug/execution/vectorized/operators/scan.h"
#include "neug/execution/vectorized/operators/select.h"
#include "neug/execution/vectorized/operators/project.h"
#include "neug/execution/vectorized/operators/result_sink.h"
#include "neug/execution/vectorized/pipeline/vec_pipeline.h"
#include "neug/execution/vectorized/pipeline/pipeline_executor.h"

using namespace neug::execution::vec;

neug::result<std::vector<std::vector<execution::Value>>> RunQuery(
    const StorageReadInterface& graph,
    const ParamsMap& params,
    label_t person_label,
    int age_prop_id,
    int name_prop_id) {

  const int TAG_PERSON = 0;
  const int TAG_NAME = 1;

  ExecutionContext ctx(graph, params);

  // 1. 构建算子
  execution::ScanParams scan_params;
  scan_params.alias = TAG_PERSON;
  scan_params.tables = {person_label};

  auto source = std::make_unique<VertexScanSource>(scan_params);

  // Select: age > 30
  auto select = std::make_unique<SelectOperator>(
      [person_label, age_prop_id, &graph](const GraphDataChunk& chunk, size_t row) -> bool {
        auto& v_vec = chunk.GetVectorByTag(0);
        // label child 通常是 ConstantVector（VertexScan 单 label 输出）
        // GetVertex 内部已处理 Constant/Flat 两种情况
        auto vertex = VertexVector::GetVertex(v_vec, row);
        auto prop = graph.GetVertexProperty(vertex.label_, vertex.vid_, age_prop_id);
        return prop.AsInt32() > 30;
      });

  // Project: 提取 name 属性
  auto project = std::make_unique<ProjectOperator>(
      std::vector<VecProjectExpr>{
          MakeVertexPropertyExpr(TAG_PERSON, TAG_NAME,
                                 DataType(DataTypeId::kVarchar),
                                 person_label, name_prop_id)
      });

  // Sink
  auto sink = std::make_unique<ResultSink>(std::vector<int>{TAG_NAME});

  // 2. 组装 Pipeline
  VecPipeline pipeline;
  pipeline.SetSource(std::move(source));
  pipeline.AddOperator(std::move(select));
  pipeline.AddOperator(std::move(project));
  pipeline.SetSink(std::move(sink));

  // 3. 执行
  auto result = PipelineExecutor::Execute(pipeline, ctx);
  if (!result) return tl::unexpected(result.error());

  // 4. 获取结果
  return ResultSink::GetResults(*pipeline.sink()->???);
  // 注: 实际需要把 sink_state 暴露出来，这里简化
}
```

---

## 8. 实现步骤分解

按依赖顺序排列：

```
Step 1: constants.h
         ↓
Step 2: validity_mask.h / .cc
         ↓
Step 3: selection_vector.h / .cc
         ↓
Step 4: vector_type.h + vector_buffer.h + 子类 + graph_vector.h / .cc
         │  ├── VectorType 枚举 (kFlat, kConstant)
         │  ├── VectorBuffer 基类 + FlatVectorBuffer
         │  ├── StringVectorBuffer (string_t[] + StringHeap)
         │  ├── StructVectorBuffer (Vertex = Struct{kUInt8, kUInt32})
         │  ├── ListVectorBuffer (Phase 1 仅定义，Path 在 Phase 3 使用)
         │  ├── GraphVector 薄壳 (DataType + VectorType + VectorBuffer)
         │  ├── Flatten() 函数 (Constant → Flat 展开)
         │  ├── helper namespaces (VertexVector, StringVector, StructVector, ListVector)
         │  ├── VertexVector::CreateSingleLabel (label child = Constant)
         │  └── GenericValue 慢路径 (SetGenericValue / GetGenericValue)
         ↓
Step 5: graph_data_chunk.h / .cc
         │  ├── Initialize / Reset / SetCardinality
         │  ├── GetVector / GetVectorByTag / FindColumnByTag
         │  ├── Compact (SelectionVector 过滤)
         │  └── AddColumn
         ↓
Step 6: execution_context.h
         ↓
Step 7: physical_operator.h (接口定义)
         ↓
Step 8: scan.h / .cc (VertexScanSource)
         ↓
Step 9: select.h / .cc (SelectOperator)
         ↓
Step 10: project.h / .cc (ProjectOperator)
          ↓
Step 11: result_sink.h / .cc
          ↓
Step 12: vec_pipeline.h
          ↓
Step 13: pipeline_executor.h / .cc
          ↓
Step 14: 单元测试
          │  ├── test_validity_mask
          │  ├── test_selection_vector
          │  ├── test_graph_vector (各类型读写)
          │  ├── test_graph_data_chunk (Initialize, Compact, AddColumn)
          │  └── test_pipeline_e2e (Scan → Select → Project → ResultSink)
          ↓
Step 15: CMakeLists.txt 集成
```

---

## 9. 测试计划

### 9.1 单元测试

| 测试目标 | 测试要点 |
|----------|----------|
| ValidityMask | AllValid 优化、SetInvalid/IsValid、边界（第 0 位、第 63 位、第 64 位） |
| SelectionVector | Identity 初始化、SetIndex/GetIndex |
| GraphVector (int64) | SetValue/GetValue、批量写入+读取 |
| GraphVector (Vertex, Flat) | VertexVector::Create / SetVertex / GetVertex、label child 和 vid child 均为 Flat |
| GraphVector (Vertex, ConstantLabel) | VertexVector::CreateSingleLabel、IsConstantLabel / GetConstantLabel、GetVertex 正确返回常量 label |
| ConstantVector + Flatten | 标量 / String / Struct ConstantVector 的创建、Flatten 展开正确性 |
| GraphVector (String) | StringVector::AddString、string_t inline / overflow 两种路径 |
| GraphVector (ValidityMask) | NULL 值设置、GetGenericValue 在 NULL 时返回 null Value |
| GraphDataChunk | Initialize、FindColumnByTag、Compact、AddColumn |
| VertexScanSource | 单 label 扫描、多 label 扫描、分 chunk 输出（验证 chunk 大小 <= STANDARD_VECTOR_SIZE） |
| SelectOperator | 全通过、全过滤、部分过滤 |
| ProjectOperator | Passthrough、属性投影 |
| PipelineExecutor | 端到端：Scan → Select → Project → ResultSink，对比旧引擎结果 |

### 9.2 正确性验证

用现有的 LDBC 数据集，执行简单查询，对比新旧引擎的输出结果（忽略顺序）。

---

## 10. Phase 1 不做的事情

明确边界，避免 scope creep：

| 不做 | 原因 | 在哪个 Phase 做 |
|------|------|:---:|
| Edge 类型的完整读写 | Phase 1 只需要 Vertex 和标量类型 | Phase 3 |
| Path 类型 | PathExpand 是 Phase 3 的内容 | Phase 3 |
| 向量化表达式求值 | Select/Project 先用逐行求值的 lambda | Phase 4 |
| Dictionary Vector（零拷贝 Slice）| 用物理 Compact 代替 | Phase 2 |
| 多线程并行 | Source/Sink 先单线程 | Phase 4 |
| Pipeline 构建器（从 PhysicalPlan 自动构建）| 手动组装即可验证 | Phase 2 |
| EdgeExpand / PathExpand | 图算子在 Phase 3 | Phase 3 |
| 与现有 PlanParser 集成 | 手动构造测试 | Phase 2 |
| DAG 调度器 | 单 pipeline 足够 | Phase 2 |

---

## 11. 与现有代码的关系

### 11.1 复用

| 现有组件 | 复用方式 |
|----------|----------|
| `DataType` / `DataTypeId` | 直接使用，不重新定义 |
| `Value` | 作为 GenericValue 慢路径使用 |
| `VertexRecord` / `EdgeRecord` / `Path` | 直接使用 |
| `StorageReadInterface` | GraphVector 的 Scan / Property 访问通过它 |
| `ScanParams` / `EdgeExpandParams` 等 | 直接复用参数结构 |
| `neug::result<T>` / `Status` | 错误处理沿用 |

### 11.2 不复用

| 现有组件 | 原因 |
|----------|------|
| `Context` | 被 `GraphDataChunk` 替代 |
| `IContextColumn` 体系 | 被 `GraphVector` 替代 |
| `IOperator::Eval` 接口 | 被 Source/Operator/Sink 三角色替代 |
| `Pipeline::Execute` | 被 `PipelineExecutor::Execute` 替代 |
| `reshuffle` 机制 | 不再需要全局重排 |
