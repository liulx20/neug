#pragma once
#include <memory>

#include "neug/common/types.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/flat_vector_buffer.h"
#include "neug/execution/vectorized/core/list_vector_buffer.h"
#include "neug/execution/vectorized/core/path_vector_buffer.h"
#include "neug/execution/vectorized/core/selection_vector.h"
#include "neug/execution/vectorized/core/string_vector_buffer.h"
#include "neug/execution/vectorized/core/struct_vector_buffer.h"
#include "neug/execution/vectorized/core/vector_buffer.h"
#include "neug/execution/vectorized/core/vector_type.h"
#include "neug/utils/property/types.h"

namespace neug::execution::vec {

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
	bool IsDictionary() const { return vector_type_ == VectorType::kDictionary; }
	bool HasSel() const { return sel_.IsSet(); }
	const SelectionVector& sel() const { return sel_; }
	SelectionVector& sel() { return sel_; }

	VectorBuffer& buffer() { return *buffer_; }
	const VectorBuffer& buffer() const { return *buffer_; }
	std::shared_ptr<VectorBuffer> shared_buffer() const { return buffer_; }
	VectorBufferType buffer_type() const { return buffer_->buffer_type(); }

	ValidityMask& validity() { return buffer_->validity(); }
	const ValidityMask& validity() const { return buffer_->validity(); }

	template <typename T>
	T* GetData() {
		return reinterpret_cast<T*>(buffer_->GetData());
	}

	template <typename T>
	const T* GetData() const {
		return reinterpret_cast<const T*>(buffer_->GetData());
	}

	void SetVectorType(VectorType vtype) { vector_type_ = vtype; }

	static GraphVector CreateWithBuffer(DataType type, VectorType vtype,
	                                    std::unique_ptr<VectorBuffer> buffer) {
		GraphVector vec;
		vec.type_ = std::move(type);
		vec.vector_type_ = vtype;
		vec.buffer_ = std::shared_ptr<VectorBuffer>(std::move(buffer));
		return vec;
	}

	static GraphVector Slice(const GraphVector& src, const sel_t* sel,
	                         size_t count);

 private:
	DataType type_;
	VectorType vector_type_ = VectorType::kFlat;
	std::shared_ptr<VectorBuffer> buffer_;
	SelectionVector sel_;
};

void Flatten(GraphVector& vec, size_t count);

// --- Helper namespaces ---

namespace StructVector {
GraphVector& GetChild(GraphVector& vec, size_t idx);
const GraphVector& GetChild(const GraphVector& vec, size_t idx);
size_t GetChildCount(const GraphVector& vec);
}  // namespace StructVector

namespace ListVector {
GraphVector& GetChild(GraphVector& vec);
const GraphVector& GetChild(const GraphVector& vec);
list_entry_t* GetEntries(GraphVector& vec);
const list_entry_t* GetEntries(const GraphVector& vec);
}  // namespace ListVector

namespace StringVector {
string_t* GetStringData(GraphVector& vec);
const string_t* GetStringData(const GraphVector& vec);
string_t AddString(GraphVector& vec, const std::string& str);
}  // namespace StringVector

namespace VertexVector {
label_t* GetLabels(GraphVector& vec);
vid_t* GetVids(GraphVector& vec);
const label_t* GetLabels(const GraphVector& vec);
const vid_t* GetVids(const GraphVector& vec);

void SetVertex(GraphVector& vec, size_t idx, label_t label, vid_t vid);
execution::VertexRecord GetVertex(const GraphVector& vec, size_t idx);

bool IsConstantLabel(const GraphVector& vec);
label_t GetConstantLabel(const GraphVector& vec);

GraphVector Create(size_t capacity = STANDARD_VECTOR_SIZE);
GraphVector CreateSingleLabel(label_t label,
                              size_t capacity = STANDARD_VECTOR_SIZE);
}  // namespace VertexVector

namespace EdgeVector {
void SetEdge(GraphVector& vec, size_t idx, const execution::EdgeRecord& edge);
execution::EdgeRecord GetEdge(const GraphVector& vec, size_t idx);

bool IsConstantLabel(const GraphVector& vec);
execution::LabelTriplet GetConstantLabel(const GraphVector& vec);
bool IsConstantDirection(const GraphVector& vec);
execution::Direction GetConstantDirection(const GraphVector& vec);

// BDML: all flat (default)
GraphVector Create(size_t capacity = STANDARD_VECTOR_SIZE);
// SDSL: labels + direction all constant
GraphVector CreateSDSL(const execution::LabelTriplet& label,
                       execution::Direction dir,
                       size_t capacity = STANDARD_VECTOR_SIZE);
// BDSL: labels constant, direction flat
GraphVector CreateBDSL(const execution::LabelTriplet& label,
                       size_t capacity = STANDARD_VECTOR_SIZE);
// SDML: direction constant, labels flat
GraphVector CreateSDML(execution::Direction dir,
                       size_t capacity = STANDARD_VECTOR_SIZE);
}  // namespace EdgeVector

namespace PathVector {
GraphVector Create(size_t capacity = STANDARD_VECTOR_SIZE);
execution::Path& GetPath(GraphVector& vec, size_t idx);
const execution::Path& GetPath(const GraphVector& vec, size_t idx);
void SetPath(GraphVector& vec, size_t idx, execution::Path path);
}  // namespace PathVector

}  // namespace neug::execution::vec
