#include "neug/execution/vectorized/core/graph_vector.h"

#include <cassert>
#include <cstring>
#include <stdexcept>

namespace neug::execution::vec {

size_t GetTypeSize(DataTypeId type_id) {
	switch (type_id) {
	case DataTypeId::kBoolean:
		return sizeof(bool);
	case DataTypeId::kInt8:
	case DataTypeId::kUInt8:
		return 1;
	case DataTypeId::kInt16:
	case DataTypeId::kUInt16:
		return 2;
	case DataTypeId::kInt32:
	case DataTypeId::kUInt32:
	case DataTypeId::kFloat:
		return 4;
	case DataTypeId::kInt64:
	case DataTypeId::kUInt64:
	case DataTypeId::kDouble:
	case DataTypeId::kDate:
	case DataTypeId::kTimestampMs:
		return 8;
	case DataTypeId::kInterval:
		return sizeof(Interval);
	default:
		return 0;
	}
}

static std::unique_ptr<VectorBuffer> CreateBuffer(DataType type,
                                                   size_t capacity) {
	switch (type.id()) {
	case DataTypeId::kVarchar:
		return std::make_unique<StringVectorBuffer>(capacity);

	case DataTypeId::kStruct:
	case DataTypeId::kVertex:
	case DataTypeId::kEdge: {
		std::vector<std::unique_ptr<GraphVector>> children;
		if (type.id() == DataTypeId::kVertex) {
			children.push_back(
			    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));
			children.push_back(
			    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
		} else if (type.id() == DataTypeId::kEdge) {
			// child 0: edge_label (uint8)
			// child 1: src vid (uint32)
			// child 2: dst vid (uint32)
			// child 3: src_label (uint8)
			// child 4: dst_label (uint8)
			// child 5: direction (uint8)
			children.push_back(
			    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));
			children.push_back(
			    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
			children.push_back(
			    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
			children.push_back(
			    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));
			children.push_back(
			    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));
			children.push_back(
			    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));
		} else {
			auto& child_types = StructType::GetChildTypes(type);
			for (auto& ct : child_types) {
				children.push_back(
				    std::make_unique<GraphVector>(ct, capacity));
			}
		}
		return std::make_unique<StructVectorBuffer>(std::move(children), capacity);
	}

	case DataTypeId::kList: {
		auto child_type = ListType::GetChildType(type);
		auto child = std::make_unique<GraphVector>(child_type, capacity);
		return std::make_unique<ListVectorBuffer>(std::move(child), capacity);
	}

	case DataTypeId::kPath:
		return std::make_unique<PathVectorBuffer>(capacity);

	default: {
		size_t type_size = GetTypeSize(type.id());
		if (type_size == 0) {
			type_size = 1;
		}
		return std::make_unique<FlatVectorBuffer>(type_size, capacity);
	}
	}
}

GraphVector::GraphVector() : type_(DataTypeId::kInvalid) {}

GraphVector::GraphVector(DataType type, size_t capacity)
    : type_(std::move(type)),
      vector_type_(VectorType::kFlat),
      buffer_(std::shared_ptr<VectorBuffer>(CreateBuffer(type_, capacity))) {}

GraphVector::~GraphVector() = default;

GraphVector::GraphVector(GraphVector&& other) noexcept
    : type_(std::move(other.type_)),
      vector_type_(other.vector_type_),
      buffer_(std::move(other.buffer_)),
      sel_(std::move(other.sel_)) {}

GraphVector& GraphVector::operator=(GraphVector&& other) noexcept {
	if (this != &other) {
		type_ = std::move(other.type_);
		vector_type_ = other.vector_type_;
		buffer_ = std::move(other.buffer_);
		sel_ = std::move(other.sel_);
	}
	return *this;
}

void GraphVector::Initialize(DataType type, size_t capacity) {
	type_ = std::move(type);
	vector_type_ = VectorType::kFlat;
	buffer_ = std::shared_ptr<VectorBuffer>(CreateBuffer(type_, capacity));
	sel_ = SelectionVector();
}

// --- Flatten ---

void Flatten(GraphVector& vec, size_t count) {
	if (vec.IsFlat()) {
		return;
	}

	if (vec.IsDictionary()) {
		assert(vec.HasSel());
		auto type = vec.type();
		auto buf_type = vec.buffer_type();
		const auto& sel = vec.sel();

		if (buf_type == VectorBufferType::kStruct) {
			auto& struct_buf =
			    static_cast<StructVectorBuffer&>(vec.buffer());
			GraphVector new_vec(type, count);
			auto& new_struct_buf =
			    static_cast<StructVectorBuffer&>(new_vec.buffer());
			for (size_t c = 0; c < struct_buf.ChildCount(); c++) {
				auto& child = struct_buf.GetChild(c);
				auto& new_child = new_struct_buf.GetChild(c);
				if (child.IsConstant()) {
					new_child.SetVectorType(VectorType::kConstant);
					size_t ts = GetTypeSize(child.type_id());
					if (ts > 0) {
						std::memcpy(new_child.buffer().GetData(),
						            child.buffer().GetData(), ts);
					}
				} else {
					size_t ts = GetTypeSize(child.type_id());
					if (ts > 0) {
						auto* src = child.buffer().GetData();
						auto* dst = new_child.buffer().GetData();
						for (size_t i = 0; i < count; i++) {
							std::memcpy(dst + i * ts,
							            src + sel.GetIndex(i) * ts, ts);
						}
					}
				}
			}
			vec = std::move(new_vec);
			return;
		}

		if (buf_type == VectorBufferType::kString) {
			GraphVector new_vec(type, count);
			auto* src_data = StringVector::GetStringData(vec);
			auto* dst_data = StringVector::GetStringData(new_vec);
			for (size_t i = 0; i < count; i++) {
				dst_data[i] = StringVector::AddString(
				    new_vec, src_data[sel.GetIndex(i)].GetString());
			}
			vec = std::move(new_vec);
			return;
		}

		if (buf_type == VectorBufferType::kPath) {
			auto& src_buf = static_cast<PathVectorBuffer&>(vec.buffer());
			GraphVector new_vec(type, count);
			auto& dst_buf = static_cast<PathVectorBuffer&>(new_vec.buffer());
			for (size_t i = 0; i < count; i++) {
				dst_buf.SetPath(i, src_buf.GetPath(sel.GetIndex(i)));
			}
			vec = std::move(new_vec);
			return;
		}

		size_t type_size = GetTypeSize(type.id());
		if (type_size == 0) {
			vec.SetVectorType(VectorType::kFlat);
			return;
		}
		auto* src_data = vec.buffer().GetData();
		GraphVector new_vec(type, count);
		auto* dst_data = new_vec.buffer().GetData();
		for (size_t i = 0; i < count; i++) {
			std::memcpy(dst_data + i * type_size,
			            src_data + sel.GetIndex(i) * type_size, type_size);
		}
		vec = std::move(new_vec);
		return;
	}

	assert(vec.IsConstant());

	auto type = vec.type();
	auto buf_type = vec.buffer_type();

	if (buf_type == VectorBufferType::kStruct) {
		auto& struct_buf =
		    static_cast<StructVectorBuffer&>(vec.buffer());
		for (size_t i = 0; i < struct_buf.ChildCount(); i++) {
			Flatten(struct_buf.GetChild(i), count);
		}
		vec.SetVectorType(VectorType::kFlat);
		return;
	}

	if (buf_type == VectorBufferType::kString) {
		auto& str_buf = static_cast<StringVectorBuffer&>(vec.buffer());
		string_t src = str_buf.GetStringData()[0];
		bool is_null = !vec.validity().AllValid() && !vec.validity().IsValid(0);

		GraphVector new_vec(type, count);
		auto& new_str_buf =
		    static_cast<StringVectorBuffer&>(new_vec.buffer());
		std::string src_str = src.GetString();
		for (size_t i = 0; i < count; i++) {
			new_str_buf.GetStringData()[i] =
			    new_str_buf.AddString(src_str);
			if (is_null) {
				new_vec.validity().SetInvalid(i);
			}
		}
		vec = std::move(new_vec);
		return;
	}

	if (buf_type == VectorBufferType::kPath) {
		auto& src_buf = static_cast<PathVectorBuffer&>(vec.buffer());
		auto src_path = src_buf.GetPath(0);
		bool is_null = !vec.validity().AllValid() && !vec.validity().IsValid(0);
		GraphVector new_vec(type, count);
		auto& dst_buf = static_cast<PathVectorBuffer&>(new_vec.buffer());
		for (size_t i = 0; i < count; i++) {
			dst_buf.SetPath(i, src_path);
			if (is_null) {
				new_vec.validity().SetInvalid(i);
			}
		}
		vec = std::move(new_vec);
		return;
	}

	// Flat scalar types
	size_t type_size = GetTypeSize(type.id());
	if (type_size == 0) {
		vec.SetVectorType(VectorType::kFlat);
		return;
	}

	auto* src_data = vec.buffer().GetData();
	bool is_null = !vec.validity().AllValid() && !vec.validity().IsValid(0);

	GraphVector new_vec(type, count);
	auto* dst_data = new_vec.buffer().GetData();
	for (size_t i = 0; i < count; i++) {
		std::memcpy(dst_data + i * type_size, src_data, type_size);
	}
	if (is_null) {
		for (size_t i = 0; i < count; i++) {
			new_vec.validity().SetInvalid(i);
		}
	}
	vec = std::move(new_vec);
}

// --- Slice ---

GraphVector GraphVector::Slice(const GraphVector& src, const sel_t* sel,
                               size_t count) {
	if (src.IsConstant()) {
		GraphVector result;
		result.type_ = src.type_;
		result.vector_type_ = VectorType::kConstant;
		result.buffer_ = src.buffer_;
		return result;
	}
	GraphVector result;
	result.type_ = src.type_;
	result.vector_type_ = VectorType::kDictionary;
	result.buffer_ = src.buffer_;
	result.sel_ = SelectionVector(count);
	if (src.IsDictionary()) {
		// Compose: new_sel[i] = src.sel[sel[i]]
		const auto& src_sel = src.sel();
		for (size_t i = 0; i < count; i++) {
			result.sel_.SetIndex(i, src_sel.GetIndex(sel[i]));
		}
	} else {
		for (size_t i = 0; i < count; i++) {
			result.sel_.SetIndex(i, sel[i]);
		}
	}
	return result;
}

// --- StructVector ---

namespace StructVector {

GraphVector& GetChild(GraphVector& vec, size_t idx) {
	auto& buf = static_cast<StructVectorBuffer&>(vec.buffer());
	return buf.GetChild(idx);
}

const GraphVector& GetChild(const GraphVector& vec, size_t idx) {
	auto& buf = static_cast<const StructVectorBuffer&>(vec.buffer());
	return buf.GetChild(idx);
}

size_t GetChildCount(const GraphVector& vec) {
	auto& buf = static_cast<const StructVectorBuffer&>(vec.buffer());
	return buf.ChildCount();
}

}  // namespace StructVector

// --- ListVector ---

namespace ListVector {

GraphVector& GetChild(GraphVector& vec) {
	auto& buf = static_cast<ListVectorBuffer&>(vec.buffer());
	return buf.GetChild();
}

const GraphVector& GetChild(const GraphVector& vec) {
	auto& buf = static_cast<const ListVectorBuffer&>(vec.buffer());
	return buf.GetChild();
}

list_entry_t* GetEntries(GraphVector& vec) {
	auto& buf = static_cast<ListVectorBuffer&>(vec.buffer());
	return buf.GetListEntries();
}

const list_entry_t* GetEntries(const GraphVector& vec) {
	auto& buf = static_cast<const ListVectorBuffer&>(vec.buffer());
	return buf.GetListEntries();
}

}  // namespace ListVector

// --- StringVector ---

namespace StringVector {

string_t* GetStringData(GraphVector& vec) {
	auto& buf = static_cast<StringVectorBuffer&>(vec.buffer());
	return buf.GetStringData();
}

const string_t* GetStringData(const GraphVector& vec) {
	auto& buf = static_cast<const StringVectorBuffer&>(vec.buffer());
	return buf.GetStringData();
}

string_t AddString(GraphVector& vec, const std::string& str) {
	auto& buf = static_cast<StringVectorBuffer&>(vec.buffer());
	return buf.AddString(str);
}

}  // namespace StringVector

// --- VertexVector ---

namespace VertexVector {

label_t* GetLabels(GraphVector& vec) {
	return StructVector::GetChild(vec, 0).GetData<label_t>();
}

vid_t* GetVids(GraphVector& vec) {
	return StructVector::GetChild(vec, 1).GetData<vid_t>();
}

const label_t* GetLabels(const GraphVector& vec) {
	return StructVector::GetChild(vec, 0).GetData<label_t>();
}

const vid_t* GetVids(const GraphVector& vec) {
	return StructVector::GetChild(vec, 1).GetData<vid_t>();
}

void SetVertex(GraphVector& vec, size_t idx, label_t label, vid_t vid) {
	auto& label_child = StructVector::GetChild(vec, 0);
	auto& vid_child = StructVector::GetChild(vec, 1);
	if (label_child.IsConstant()) {
		label_child.GetData<label_t>()[0] = label;
	} else {
		label_child.GetData<label_t>()[idx] = label;
	}
	vid_child.GetData<vid_t>()[idx] = vid;
}

execution::VertexRecord GetVertex(const GraphVector& vec, size_t idx) {
	auto& label_child = StructVector::GetChild(vec, 0);
	auto& vid_child = StructVector::GetChild(vec, 1);
	label_t label;
	if (label_child.IsConstant()) {
		label = label_child.GetData<label_t>()[0];
	} else {
		label = label_child.GetData<label_t>()[idx];
	}
	vid_t vid = vid_child.GetData<vid_t>()[idx];
	return execution::VertexRecord(label, vid);
}

bool IsConstantLabel(const GraphVector& vec) {
	return StructVector::GetChild(vec, 0).IsConstant();
}

label_t GetConstantLabel(const GraphVector& vec) {
	auto& label_child = StructVector::GetChild(vec, 0);
	assert(label_child.IsConstant());
	return label_child.GetData<label_t>()[0];
}

GraphVector Create(size_t capacity) {
	return GraphVector(DataType(DataTypeId::kVertex), capacity);
}

GraphVector CreateSingleLabel(label_t label, size_t capacity) {
	auto label_vec = std::make_unique<GraphVector>(
	    DataType(DataTypeId::kUInt8), 1);
	label_vec->SetVectorType(VectorType::kConstant);
	label_vec->GetData<label_t>()[0] = label;

	auto vid_vec = std::make_unique<GraphVector>(
	    DataType(DataTypeId::kUInt32), capacity);

	std::vector<std::unique_ptr<GraphVector>> children;
	children.push_back(std::move(label_vec));
	children.push_back(std::move(vid_vec));

	auto buf = std::make_unique<StructVectorBuffer>(
	    std::move(children), capacity);

	return GraphVector::CreateWithBuffer(DataType(DataTypeId::kVertex),
	                                     VectorType::kFlat, std::move(buf));
}

}  // namespace VertexVector

// --- EdgeVector ---

namespace EdgeVector {

static void SetChild(GraphVector& child, size_t idx, label_t val) {
	if (child.IsConstant()) {
		child.GetData<label_t>()[0] = val;
	} else {
		child.GetData<label_t>()[idx] = val;
	}
}

static void SetChild(GraphVector& child, size_t idx, vid_t val) {
	if (child.IsConstant()) {
		child.GetData<vid_t>()[0] = val;
	} else {
		child.GetData<vid_t>()[idx] = val;
	}
}

static label_t GetChildLabel(const GraphVector& child, size_t idx) {
	return child.IsConstant() ? child.GetData<label_t>()[0]
	                          : child.GetData<label_t>()[idx];
}

static vid_t GetChildVid(const GraphVector& child, size_t idx) {
	return child.IsConstant() ? child.GetData<vid_t>()[0]
	                          : child.GetData<vid_t>()[idx];
}

void SetEdge(GraphVector& vec, size_t idx,
             const execution::EdgeRecord& edge) {
	SetChild(StructVector::GetChild(vec, 0), idx, edge.label.edge_label);
	SetChild(StructVector::GetChild(vec, 1), idx, edge.src);
	SetChild(StructVector::GetChild(vec, 2), idx, edge.dst);
	SetChild(StructVector::GetChild(vec, 3), idx, edge.label.src_label);
	SetChild(StructVector::GetChild(vec, 4), idx, edge.label.dst_label);
	SetChild(StructVector::GetChild(vec, 5), idx,
	         static_cast<label_t>(edge.dir));
}

execution::EdgeRecord GetEdge(const GraphVector& vec, size_t idx) {
	execution::EdgeRecord edge;
	edge.label.edge_label = GetChildLabel(StructVector::GetChild(vec, 0), idx);
	edge.src = GetChildVid(StructVector::GetChild(vec, 1), idx);
	edge.dst = GetChildVid(StructVector::GetChild(vec, 2), idx);
	edge.label.src_label = GetChildLabel(StructVector::GetChild(vec, 3), idx);
	edge.label.dst_label = GetChildLabel(StructVector::GetChild(vec, 4), idx);
	edge.prop = nullptr;
	edge.dir = static_cast<execution::Direction>(
	    GetChildLabel(StructVector::GetChild(vec, 5), idx));
	return edge;
}

bool IsConstantLabel(const GraphVector& vec) {
	return StructVector::GetChild(vec, 0).IsConstant() &&
	       StructVector::GetChild(vec, 3).IsConstant() &&
	       StructVector::GetChild(vec, 4).IsConstant();
}

execution::LabelTriplet GetConstantLabel(const GraphVector& vec) {
	return execution::LabelTriplet(
	    StructVector::GetChild(vec, 3).GetData<label_t>()[0],
	    StructVector::GetChild(vec, 4).GetData<label_t>()[0],
	    StructVector::GetChild(vec, 0).GetData<label_t>()[0]);
}

bool IsConstantDirection(const GraphVector& vec) {
	return StructVector::GetChild(vec, 5).IsConstant();
}

execution::Direction GetConstantDirection(const GraphVector& vec) {
	return static_cast<execution::Direction>(
	    StructVector::GetChild(vec, 5).GetData<label_t>()[0]);
}

GraphVector Create(size_t capacity) {
	return GraphVector(DataType(DataTypeId::kEdge), capacity);
}

static std::unique_ptr<GraphVector> MakeConstantU8(label_t val) {
	auto v = std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), 1);
	v->SetVectorType(VectorType::kConstant);
	v->GetData<label_t>()[0] = val;
	return v;
}

GraphVector CreateSDSL(const execution::LabelTriplet& label,
                       execution::Direction dir, size_t capacity) {
	std::vector<std::unique_ptr<GraphVector>> children;
	children.push_back(MakeConstantU8(label.edge_label));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
	children.push_back(MakeConstantU8(label.src_label));
	children.push_back(MakeConstantU8(label.dst_label));
	children.push_back(MakeConstantU8(static_cast<label_t>(dir)));

	auto buf = std::make_unique<StructVectorBuffer>(
	    std::move(children), capacity);
	return GraphVector::CreateWithBuffer(DataType(DataTypeId::kEdge),
	                                     VectorType::kFlat, std::move(buf));
}

GraphVector CreateBDSL(const execution::LabelTriplet& label,
                       size_t capacity) {
	std::vector<std::unique_ptr<GraphVector>> children;
	children.push_back(MakeConstantU8(label.edge_label));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
	children.push_back(MakeConstantU8(label.src_label));
	children.push_back(MakeConstantU8(label.dst_label));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));

	auto buf = std::make_unique<StructVectorBuffer>(
	    std::move(children), capacity);
	return GraphVector::CreateWithBuffer(DataType(DataTypeId::kEdge),
	                                     VectorType::kFlat, std::move(buf));
}

GraphVector CreateSDML(execution::Direction dir, size_t capacity) {
	std::vector<std::unique_ptr<GraphVector>> children;
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt32), capacity));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));
	children.push_back(
	    std::make_unique<GraphVector>(DataType(DataTypeId::kUInt8), capacity));
	children.push_back(MakeConstantU8(static_cast<label_t>(dir)));

	auto buf = std::make_unique<StructVectorBuffer>(
	    std::move(children), capacity);
	return GraphVector::CreateWithBuffer(DataType(DataTypeId::kEdge),
	                                     VectorType::kFlat, std::move(buf));
}

}  // namespace EdgeVector

// --- PathVector ---

namespace PathVector {

GraphVector Create(size_t capacity) {
	return GraphVector(DataType(DataTypeId::kPath), capacity);
}

execution::Path& GetPath(GraphVector& vec, size_t idx) {
	auto& buf = static_cast<PathVectorBuffer&>(vec.buffer());
	return buf.GetPath(idx);
}

const execution::Path& GetPath(const GraphVector& vec, size_t idx) {
	auto& buf = static_cast<const PathVectorBuffer&>(vec.buffer());
	return buf.GetPath(idx);
}

void SetPath(GraphVector& vec, size_t idx, execution::Path path) {
	auto& buf = static_cast<PathVectorBuffer&>(vec.buffer());
	buf.SetPath(idx, std::move(path));
}

}  // namespace PathVector

}  // namespace neug::execution::vec
