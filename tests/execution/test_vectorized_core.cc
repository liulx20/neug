#include <gtest/gtest.h>

#include "neug/execution/vectorized/core/constants.h"
#include "neug/execution/vectorized/core/graph_data_chunk.h"
#include "neug/execution/vectorized/core/graph_vector.h"
#include "neug/execution/vectorized/core/selection_vector.h"
#include "neug/execution/vectorized/core/validity_mask.h"

using namespace neug::execution::vec;
using neug::DataType;
using neug::DataTypeId;

// ============================================================
// ValidityMask
// ============================================================

TEST(VectorizedValidityMask, AllValidByDefault) {
	ValidityMask mask(128);
	EXPECT_TRUE(mask.AllValid());
	for (size_t i = 0; i < 128; i++) {
		EXPECT_TRUE(mask.IsValid(i));
	}
}

TEST(VectorizedValidityMask, SetInvalidAllocates) {
	ValidityMask mask(128);
	EXPECT_TRUE(mask.AllValid());
	mask.SetInvalid(10);
	EXPECT_FALSE(mask.AllValid());
	EXPECT_FALSE(mask.IsValid(10));
	EXPECT_TRUE(mask.IsValid(0));
	EXPECT_TRUE(mask.IsValid(9));
	EXPECT_TRUE(mask.IsValid(11));
}

TEST(VectorizedValidityMask, BoundaryBits) {
	ValidityMask mask(128);
	mask.SetInvalid(0);
	mask.SetInvalid(63);
	mask.SetInvalid(64);
	mask.SetInvalid(127);

	EXPECT_FALSE(mask.IsValid(0));
	EXPECT_FALSE(mask.IsValid(63));
	EXPECT_FALSE(mask.IsValid(64));
	EXPECT_FALSE(mask.IsValid(127));

	EXPECT_TRUE(mask.IsValid(1));
	EXPECT_TRUE(mask.IsValid(62));
	EXPECT_TRUE(mask.IsValid(65));
	EXPECT_TRUE(mask.IsValid(126));
}

TEST(VectorizedValidityMask, SetValidRestores) {
	ValidityMask mask(64);
	mask.SetInvalid(5);
	EXPECT_FALSE(mask.IsValid(5));
	mask.SetValid(5);
	EXPECT_TRUE(mask.IsValid(5));
}

TEST(VectorizedValidityMask, Reset) {
	ValidityMask mask(64);
	mask.SetInvalid(5);
	EXPECT_FALSE(mask.AllValid());
	mask.Reset();
	EXPECT_TRUE(mask.AllValid());
	EXPECT_TRUE(mask.IsValid(5));
}

TEST(VectorizedValidityMask, SetAllValid) {
	ValidityMask mask(128);
	mask.SetInvalid(10);
	mask.SetInvalid(70);
	mask.SetAllValid();
	EXPECT_TRUE(mask.IsValid(10));
	EXPECT_TRUE(mask.IsValid(70));
}

TEST(VectorizedValidityMask, SetAllInvalid) {
	ValidityMask mask(128);
	mask.SetAllInvalid();
	for (size_t i = 0; i < 128; i++) {
		EXPECT_FALSE(mask.IsValid(i));
	}
}

// ============================================================
// SelectionVector
// ============================================================

TEST(VectorizedSelectionVector, DefaultNotSet) {
	SelectionVector sel;
	EXPECT_FALSE(sel.IsSet());
	EXPECT_EQ(sel.count(), 0u);
}

TEST(VectorizedSelectionVector, OwnedCreation) {
	SelectionVector sel(10);
	EXPECT_TRUE(sel.IsSet());
	EXPECT_EQ(sel.count(), 10u);

	for (size_t i = 0; i < 10; i++) {
		sel.SetIndex(i, static_cast<sel_t>(i * 2));
	}
	for (size_t i = 0; i < 10; i++) {
		EXPECT_EQ(sel.GetIndex(i), i * 2);
	}
}

TEST(VectorizedSelectionVector, NonOwning) {
	sel_t data[5] = {10, 20, 30, 40, 50};
	SelectionVector sel(data, 5);
	EXPECT_TRUE(sel.IsSet());
	EXPECT_EQ(sel.count(), 5u);
	EXPECT_EQ(sel.GetIndex(2), 30u);
}

TEST(VectorizedSelectionVector, InitializeIdentity) {
	SelectionVector sel;
	sel.InitializeIdentity(100);
	EXPECT_TRUE(sel.IsSet());
	EXPECT_EQ(sel.count(), 100u);
	for (size_t i = 0; i < 100; i++) {
		EXPECT_EQ(sel.GetIndex(i), i);
	}
}

// ============================================================
// GraphVector - Flat Scalars
// ============================================================

TEST(VectorizedGraphVector, Int32FlatVector) {
	GraphVector vec{DataType(DataTypeId::kInt32)};
	EXPECT_EQ(vec.type_id(), DataTypeId::kInt32);
	EXPECT_EQ(vec.buffer_type(), VectorBufferType::kFlat);
	EXPECT_TRUE(vec.IsFlat());

	auto* data = vec.GetData<int32_t>();
	ASSERT_NE(data, nullptr);

	for (size_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		data[i] = static_cast<int32_t>(i * 3 - 100);
	}
	for (size_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		EXPECT_EQ(data[i], static_cast<int32_t>(i * 3 - 100));
	}
}

TEST(VectorizedGraphVector, Int64FlatVector) {
	GraphVector vec{DataType(DataTypeId::kInt64)};
	auto* data = vec.GetData<int64_t>();
	ASSERT_NE(data, nullptr);

	data[0] = -999999999LL;
	data[1] = 999999999LL;
	EXPECT_EQ(data[0], -999999999LL);
	EXPECT_EQ(data[1], 999999999LL);
}

TEST(VectorizedGraphVector, DoubleFlatVector) {
	GraphVector vec{DataType(DataTypeId::kDouble)};
	auto* data = vec.GetData<double>();
	data[0] = 3.14159;
	data[1] = -2.71828;
	EXPECT_DOUBLE_EQ(data[0], 3.14159);
	EXPECT_DOUBLE_EQ(data[1], -2.71828);
}

TEST(VectorizedGraphVector, BoolFlatVector) {
	GraphVector vec{DataType(DataTypeId::kBoolean)};
	auto* data = vec.GetData<bool>();
	data[0] = true;
	data[1] = false;
	data[2] = true;
	EXPECT_TRUE(data[0]);
	EXPECT_FALSE(data[1]);
	EXPECT_TRUE(data[2]);
}

TEST(VectorizedGraphVector, FlatVectorWithNulls) {
	GraphVector vec{DataType(DataTypeId::kInt32)};
	auto* data = vec.GetData<int32_t>();
	data[0] = 42;
	data[1] = 0;
	data[2] = 99;

	EXPECT_TRUE(vec.validity().AllValid());
	vec.validity().SetInvalid(1);
	EXPECT_TRUE(vec.validity().IsValid(0));
	EXPECT_FALSE(vec.validity().IsValid(1));
	EXPECT_TRUE(vec.validity().IsValid(2));
}

TEST(VectorizedGraphVector, MoveSemantics) {
	GraphVector vec1{DataType(DataTypeId::kInt32)};
	vec1.GetData<int32_t>()[0] = 42;

	GraphVector vec2(std::move(vec1));
	EXPECT_EQ(vec2.GetData<int32_t>()[0], 42);
	EXPECT_EQ(vec2.type_id(), DataTypeId::kInt32);
}

// ============================================================
// GraphVector - String
// ============================================================

TEST(VectorizedGraphVector, StringVectorShort) {
	GraphVector vec{DataType(DataTypeId::kVarchar)};
	EXPECT_EQ(vec.buffer_type(), VectorBufferType::kString);

	auto& buf = static_cast<StringVectorBuffer&>(vec.buffer());
	buf.GetStringData()[0] = buf.AddString("hello");
	buf.GetStringData()[1] = buf.AddString("world");

	EXPECT_EQ(buf.GetStringData()[0].GetString(), "hello");
	EXPECT_EQ(buf.GetStringData()[1].GetString(), "world");
	EXPECT_LE(buf.GetStringData()[0].length, string_t::INLINE_LENGTH);
}

TEST(VectorizedGraphVector, StringVectorLong) {
	GraphVector vec{DataType(DataTypeId::kVarchar)};
	auto& buf = static_cast<StringVectorBuffer&>(vec.buffer());

	std::string long_str = "this is a very long string that exceeds twelve bytes";
	buf.GetStringData()[0] = buf.AddString(long_str);

	EXPECT_EQ(buf.GetStringData()[0].GetString(), long_str);
	EXPECT_GT(buf.GetStringData()[0].length, string_t::INLINE_LENGTH);
}

TEST(VectorizedGraphVector, StringVectorHelper) {
	GraphVector vec{DataType(DataTypeId::kVarchar)};

	auto* data = StringVector::GetStringData(vec);
	data[0] = StringVector::AddString(vec, "abc");
	data[1] = StringVector::AddString(vec, "a long string for overflow test!!");

	EXPECT_EQ(data[0].GetString(), "abc");
	EXPECT_EQ(data[1].GetString(), "a long string for overflow test!!");
}

// ============================================================
// GraphVector - Struct
// ============================================================

TEST(VectorizedGraphVector, StructVectorBasic) {
	GraphVector vec{DataType(DataTypeId::kVertex)};
	EXPECT_EQ(vec.buffer_type(), VectorBufferType::kStruct);
	EXPECT_EQ(StructVector::GetChildCount(vec), 2u);

	auto& label_child = StructVector::GetChild(vec, 0);
	auto& vid_child = StructVector::GetChild(vec, 1);
	EXPECT_EQ(label_child.type_id(), DataTypeId::kUInt8);
	EXPECT_EQ(vid_child.type_id(), DataTypeId::kUInt32);
}

// ============================================================
// GraphVector - Vertex (Flat)
// ============================================================

TEST(VectorizedVertexVector, CreateAndAccess) {
	auto vec = VertexVector::Create();
	EXPECT_EQ(vec.type_id(), DataTypeId::kVertex);

	auto* labels = VertexVector::GetLabels(vec);
	auto* vids = VertexVector::GetVids(vec);

	for (size_t i = 0; i < 100; i++) {
		labels[i] = static_cast<neug::label_t>(i % 5);
		vids[i] = static_cast<neug::vid_t>(i * 10);
	}

	for (size_t i = 0; i < 100; i++) {
		auto v = VertexVector::GetVertex(vec, i);
		EXPECT_EQ(v.label_, i % 5);
		EXPECT_EQ(v.vid_, i * 10);
	}
}

TEST(VectorizedVertexVector, SetVertexFlat) {
	auto vec = VertexVector::Create();
	VertexVector::SetVertex(vec, 0, 1, 100);
	VertexVector::SetVertex(vec, 1, 2, 200);
	VertexVector::SetVertex(vec, 2, 1, 300);

	auto v0 = VertexVector::GetVertex(vec, 0);
	auto v1 = VertexVector::GetVertex(vec, 1);
	auto v2 = VertexVector::GetVertex(vec, 2);

	EXPECT_EQ(v0.label_, 1);
	EXPECT_EQ(v0.vid_, 100u);
	EXPECT_EQ(v1.label_, 2);
	EXPECT_EQ(v1.vid_, 200u);
	EXPECT_EQ(v2.label_, 1);
	EXPECT_EQ(v2.vid_, 300u);
}

// ============================================================
// GraphVector - Vertex (ConstantLabel)
// ============================================================

TEST(VectorizedVertexVector, CreateSingleLabel) {
	auto vec = VertexVector::CreateSingleLabel(5);

	EXPECT_TRUE(VertexVector::IsConstantLabel(vec));
	EXPECT_EQ(VertexVector::GetConstantLabel(vec), 5);

	auto& label_child = StructVector::GetChild(vec, 0);
	EXPECT_TRUE(label_child.IsConstant());

	auto* vids = VertexVector::GetVids(vec);
	for (size_t i = 0; i < 100; i++) {
		vids[i] = static_cast<neug::vid_t>(i + 1);
	}

	for (size_t i = 0; i < 100; i++) {
		auto v = VertexVector::GetVertex(vec, i);
		EXPECT_EQ(v.label_, 5);
		EXPECT_EQ(v.vid_, i + 1);
	}
}

TEST(VectorizedVertexVector, SetVertexWithConstantLabel) {
	auto vec = VertexVector::CreateSingleLabel(3);

	VertexVector::SetVertex(vec, 0, 3, 100);
	VertexVector::SetVertex(vec, 1, 3, 200);

	auto v0 = VertexVector::GetVertex(vec, 0);
	auto v1 = VertexVector::GetVertex(vec, 1);
	EXPECT_EQ(v0.label_, 3);
	EXPECT_EQ(v0.vid_, 100u);
	EXPECT_EQ(v1.label_, 3);
	EXPECT_EQ(v1.vid_, 200u);
}

// ============================================================
// Flatten
// ============================================================

TEST(VectorizedFlatten, FlatVectorIsNoop) {
	GraphVector vec{DataType(DataTypeId::kInt32)};
	vec.GetData<int32_t>()[0] = 42;
	Flatten(vec, 10);
	EXPECT_TRUE(vec.IsFlat());
	EXPECT_EQ(vec.GetData<int32_t>()[0], 42);
}

TEST(VectorizedFlatten, ConstantScalar) {
	GraphVector vec{DataType(DataTypeId::kInt32), 1};
	vec.SetVectorType(VectorType::kConstant);
	vec.GetData<int32_t>()[0] = 77;

	Flatten(vec, 100);
	EXPECT_TRUE(vec.IsFlat());

	auto* data = vec.GetData<int32_t>();
	for (size_t i = 0; i < 100; i++) {
		EXPECT_EQ(data[i], 77);
	}
}

TEST(VectorizedFlatten, ConstantVertexStruct) {
	auto vec = VertexVector::CreateSingleLabel(8);
	auto* vids = VertexVector::GetVids(vec);
	for (size_t i = 0; i < 50; i++) {
		vids[i] = static_cast<neug::vid_t>(i);
	}

	EXPECT_TRUE(VertexVector::IsConstantLabel(vec));

	// Flatten the label child
	auto& label_child = StructVector::GetChild(vec, 0);
	Flatten(label_child, 50);

	EXPECT_TRUE(label_child.IsFlat());
	auto* labels = label_child.GetData<neug::label_t>();
	for (size_t i = 0; i < 50; i++) {
		EXPECT_EQ(labels[i], 8);
	}
}

// ============================================================
// GraphVector - Edge
// ============================================================

TEST(VectorizedEdgeVector, CreateAndAccess) {
	auto vec = EdgeVector::Create();
	EXPECT_EQ(vec.type_id(), DataTypeId::kEdge);
	EXPECT_EQ(StructVector::GetChildCount(vec), 6u);

	neug::execution::EdgeRecord edge;
	edge.label.edge_label = 1;
	edge.src = 100;
	edge.dst = 200;
	edge.label.src_label = 2;
	edge.label.dst_label = 3;
	edge.prop = nullptr;
	edge.dir = neug::execution::Direction::kOut;

	EdgeVector::SetEdge(vec, 0, edge);
	auto result = EdgeVector::GetEdge(vec, 0);

	EXPECT_EQ(result.label.edge_label, 1);
	EXPECT_EQ(result.src, 100u);
	EXPECT_EQ(result.dst, 200u);
	EXPECT_EQ(result.label.src_label, 2);
	EXPECT_EQ(result.label.dst_label, 3);
	EXPECT_EQ(result.dir, neug::execution::Direction::kOut);
}

TEST(VectorizedEdgeVector, SDSL) {
	using namespace neug::execution;
	LabelTriplet label(2, 3, 1);
	auto vec = EdgeVector::CreateSDSL(label, Direction::kOut);

	EXPECT_TRUE(EdgeVector::IsConstantLabel(vec));
	EXPECT_TRUE(EdgeVector::IsConstantDirection(vec));
	EXPECT_EQ(EdgeVector::GetConstantDirection(vec), Direction::kOut);

	auto cl = EdgeVector::GetConstantLabel(vec);
	EXPECT_EQ(cl.edge_label, 1);
	EXPECT_EQ(cl.src_label, 2);
	EXPECT_EQ(cl.dst_label, 3);

	for (size_t i = 0; i < 100; i++) {
		EdgeRecord e;
		e.label = label;
		e.src = static_cast<neug::vid_t>(i);
		e.dst = static_cast<neug::vid_t>(i + 1000);
		e.prop = nullptr;
		e.dir = Direction::kOut;
		EdgeVector::SetEdge(vec, i, e);
	}

	for (size_t i = 0; i < 100; i++) {
		auto r = EdgeVector::GetEdge(vec, i);
		EXPECT_EQ(r.label.edge_label, 1);
		EXPECT_EQ(r.label.src_label, 2);
		EXPECT_EQ(r.label.dst_label, 3);
		EXPECT_EQ(r.src, i);
		EXPECT_EQ(r.dst, i + 1000);
		EXPECT_EQ(r.dir, Direction::kOut);
	}
}

TEST(VectorizedEdgeVector, BDSL) {
	using namespace neug::execution;
	LabelTriplet label(2, 3, 1);
	auto vec = EdgeVector::CreateBDSL(label);

	EXPECT_TRUE(EdgeVector::IsConstantLabel(vec));
	EXPECT_FALSE(EdgeVector::IsConstantDirection(vec));

	EdgeRecord e1;
	e1.label = label;
	e1.src = 10;
	e1.dst = 20;
	e1.prop = nullptr;
	e1.dir = Direction::kOut;
	EdgeVector::SetEdge(vec, 0, e1);

	EdgeRecord e2;
	e2.label = label;
	e2.src = 20;
	e2.dst = 10;
	e2.prop = nullptr;
	e2.dir = Direction::kIn;
	EdgeVector::SetEdge(vec, 1, e2);

	auto r0 = EdgeVector::GetEdge(vec, 0);
	auto r1 = EdgeVector::GetEdge(vec, 1);
	EXPECT_EQ(r0.dir, Direction::kOut);
	EXPECT_EQ(r1.dir, Direction::kIn);
	EXPECT_EQ(r0.label.edge_label, r1.label.edge_label);
}

TEST(VectorizedEdgeVector, SDML) {
	using namespace neug::execution;
	auto vec = EdgeVector::CreateSDML(Direction::kOut);

	EXPECT_FALSE(EdgeVector::IsConstantLabel(vec));
	EXPECT_TRUE(EdgeVector::IsConstantDirection(vec));
	EXPECT_EQ(EdgeVector::GetConstantDirection(vec), Direction::kOut);

	EdgeRecord e1;
	e1.label = LabelTriplet(1, 2, 10);
	e1.src = 100;
	e1.dst = 200;
	e1.prop = nullptr;
	e1.dir = Direction::kOut;
	EdgeVector::SetEdge(vec, 0, e1);

	EdgeRecord e2;
	e2.label = LabelTriplet(3, 4, 20);
	e2.src = 300;
	e2.dst = 400;
	e2.prop = nullptr;
	e2.dir = Direction::kOut;
	EdgeVector::SetEdge(vec, 1, e2);

	auto r0 = EdgeVector::GetEdge(vec, 0);
	auto r1 = EdgeVector::GetEdge(vec, 1);
	EXPECT_EQ(r0.label.edge_label, 10);
	EXPECT_EQ(r1.label.edge_label, 20);
	EXPECT_EQ(r0.dir, Direction::kOut);
	EXPECT_EQ(r1.dir, Direction::kOut);
}

TEST(VectorizedEdgeVector, BDML) {
	using namespace neug::execution;
	auto vec = EdgeVector::Create();

	EXPECT_FALSE(EdgeVector::IsConstantLabel(vec));
	EXPECT_FALSE(EdgeVector::IsConstantDirection(vec));

	EdgeRecord e1;
	e1.label = LabelTriplet(1, 2, 10);
	e1.src = 100;
	e1.dst = 200;
	e1.prop = nullptr;
	e1.dir = Direction::kOut;
	EdgeVector::SetEdge(vec, 0, e1);

	EdgeRecord e2;
	e2.label = LabelTriplet(3, 4, 20);
	e2.src = 300;
	e2.dst = 400;
	e2.prop = nullptr;
	e2.dir = Direction::kIn;
	EdgeVector::SetEdge(vec, 1, e2);

	auto r0 = EdgeVector::GetEdge(vec, 0);
	auto r1 = EdgeVector::GetEdge(vec, 1);
	EXPECT_EQ(r0.label.edge_label, 10);
	EXPECT_EQ(r1.label.edge_label, 20);
	EXPECT_EQ(r0.dir, Direction::kOut);
	EXPECT_EQ(r1.dir, Direction::kIn);
}

// ============================================================
// GraphDataChunk
// ============================================================

TEST(VectorizedGraphDataChunk, InitializeAndAccess) {
	GraphDataChunk chunk;
	chunk.Initialize({{0, DataType(DataTypeId::kVertex)},
	                  {1, DataType(DataTypeId::kInt32)}});

	EXPECT_EQ(chunk.ColumnCount(), 2u);
	EXPECT_EQ(chunk.size(), 0u);
	EXPECT_EQ(chunk.GetTag(0), 0);
	EXPECT_EQ(chunk.GetTag(1), 1);
	EXPECT_EQ(chunk.FindColumnByTag(0), 0);
	EXPECT_EQ(chunk.FindColumnByTag(1), 1);
	EXPECT_EQ(chunk.FindColumnByTag(99), -1);
}

TEST(VectorizedGraphDataChunk, GetVectorByTag) {
	GraphDataChunk chunk;
	chunk.Initialize({{10, DataType(DataTypeId::kInt32)},
	                  {20, DataType(DataTypeId::kInt64)}});

	auto& int_vec = chunk.GetVectorByTag(10);
	auto& long_vec = chunk.GetVectorByTag(20);

	EXPECT_EQ(int_vec.type_id(), DataTypeId::kInt32);
	EXPECT_EQ(long_vec.type_id(), DataTypeId::kInt64);
}

TEST(VectorizedGraphDataChunk, SetCardinality) {
	GraphDataChunk chunk;
	chunk.Initialize({{0, DataType(DataTypeId::kInt32)}});

	auto* data = chunk.GetVector(0).GetData<int32_t>();
	for (size_t i = 0; i < 100; i++) {
		data[i] = static_cast<int32_t>(i);
	}
	chunk.SetCardinality(100);
	EXPECT_EQ(chunk.size(), 100u);
}

TEST(VectorizedGraphDataChunk, Compact) {
	GraphDataChunk chunk;
	chunk.Initialize({{0, DataType(DataTypeId::kInt32)}});

	auto* data = chunk.GetVector(0).GetData<int32_t>();
	for (size_t i = 0; i < 10; i++) {
		data[i] = static_cast<int32_t>(i * 10);
	}
	chunk.SetCardinality(10);

	// Keep only even-indexed rows: 0, 2, 4, 6, 8
	SelectionVector sel(5);
	sel.SetIndex(0, 0);
	sel.SetIndex(1, 2);
	sel.SetIndex(2, 4);
	sel.SetIndex(3, 6);
	sel.SetIndex(4, 8);

	chunk.Compact(sel, 5);
	EXPECT_EQ(chunk.size(), 5u);

	auto* compacted = chunk.GetVector(0).GetData<int32_t>();
	EXPECT_EQ(compacted[0], 0);
	EXPECT_EQ(compacted[1], 20);
	EXPECT_EQ(compacted[2], 40);
	EXPECT_EQ(compacted[3], 60);
	EXPECT_EQ(compacted[4], 80);
}

TEST(VectorizedGraphDataChunk, CompactVertex) {
	GraphDataChunk chunk;
	chunk.Initialize({{0, DataType(DataTypeId::kVertex)}});

	auto& vertex_vec = chunk.GetVector(0);
	auto* labels = VertexVector::GetLabels(vertex_vec);
	auto* vids = VertexVector::GetVids(vertex_vec);
	for (size_t i = 0; i < 6; i++) {
		labels[i] = static_cast<neug::label_t>(i);
		vids[i] = static_cast<neug::vid_t>(i * 100);
	}
	chunk.SetCardinality(6);

	SelectionVector sel(3);
	sel.SetIndex(0, 1);
	sel.SetIndex(1, 3);
	sel.SetIndex(2, 5);

	chunk.Compact(sel, 3);
	EXPECT_EQ(chunk.size(), 3u);

	auto v0 = VertexVector::GetVertex(chunk.GetVector(0), 0);
	auto v1 = VertexVector::GetVertex(chunk.GetVector(0), 1);
	auto v2 = VertexVector::GetVertex(chunk.GetVector(0), 2);
	EXPECT_EQ(v0.label_, 1);
	EXPECT_EQ(v0.vid_, 100u);
	EXPECT_EQ(v1.label_, 3);
	EXPECT_EQ(v1.vid_, 300u);
	EXPECT_EQ(v2.label_, 5);
	EXPECT_EQ(v2.vid_, 500u);
}

TEST(VectorizedGraphDataChunk, AddColumn) {
	GraphDataChunk chunk;
	chunk.Initialize({{0, DataType(DataTypeId::kInt32)}});
	EXPECT_EQ(chunk.ColumnCount(), 1u);

	chunk.AddColumn(5, DataType(DataTypeId::kDouble));
	EXPECT_EQ(chunk.ColumnCount(), 2u);
	EXPECT_EQ(chunk.FindColumnByTag(5), 1);

	auto& new_vec = chunk.GetVectorByTag(5);
	EXPECT_EQ(new_vec.type_id(), DataTypeId::kDouble);
}

TEST(VectorizedGraphDataChunk, Reset) {
	GraphDataChunk chunk;
	chunk.Initialize({{0, DataType(DataTypeId::kInt32)}});
	chunk.SetCardinality(100);
	EXPECT_EQ(chunk.size(), 100u);
	chunk.Reset();
	EXPECT_EQ(chunk.size(), 0u);
	EXPECT_EQ(chunk.ColumnCount(), 1u);
}

TEST(VectorizedGraphDataChunk, GetTypes) {
	GraphDataChunk chunk;
	chunk.Initialize({{0, DataType(DataTypeId::kInt32)},
	                  {1, DataType(DataTypeId::kVertex)}});
	auto types = chunk.GetTypes();
	EXPECT_EQ(types.size(), 2u);
	EXPECT_EQ(types[0].id(), DataTypeId::kInt32);
	EXPECT_EQ(types[1].id(), DataTypeId::kVertex);
}

TEST(VectorizedGraphDataChunk, ToString) {
	GraphDataChunk chunk;
	chunk.Initialize({{0, DataType(DataTypeId::kInt32)},
	                  {1, DataType(DataTypeId::kVertex)}});
	chunk.SetCardinality(50);
	auto str = chunk.ToString();
	EXPECT_NE(str.find("rows=50"), std::string::npos);
	EXPECT_NE(str.find("cols=2"), std::string::npos);
}

// ============================================================
// Integration: GraphDataChunk with ConstantLabel Vertex
// ============================================================

TEST(VectorizedIntegration, ChunkWithConstantLabelVertex) {
	GraphDataChunk chunk;

	auto vertex_vec = VertexVector::CreateSingleLabel(7);
	auto* vids = VertexVector::GetVids(vertex_vec);
	for (size_t i = 0; i < 50; i++) {
		vids[i] = static_cast<neug::vid_t>(i + 1);
	}

	// Build the chunk manually
	chunk.AddColumn(0, DataType(DataTypeId::kInt32));
	auto* ages = chunk.GetVector(0).GetData<int32_t>();
	for (size_t i = 0; i < 50; i++) {
		ages[i] = static_cast<int32_t>(20 + i);
	}
	chunk.SetCardinality(50);

	// Verify the separate vertex vector
	for (size_t i = 0; i < 50; i++) {
		auto v = VertexVector::GetVertex(vertex_vec, i);
		EXPECT_EQ(v.label_, 7);
		EXPECT_EQ(v.vid_, i + 1);
	}
}

// ============================================================
// GetTypeSize
// ============================================================

TEST(VectorizedGetTypeSize, KnownTypes) {
	EXPECT_EQ(GetTypeSize(DataTypeId::kBoolean), sizeof(bool));
	EXPECT_EQ(GetTypeSize(DataTypeId::kInt8), 1u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kUInt8), 1u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kInt16), 2u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kUInt16), 2u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kInt32), 4u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kUInt32), 4u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kFloat), 4u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kInt64), 8u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kUInt64), 8u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kDouble), 8u);
}

TEST(VectorizedGetTypeSize, ComplexTypesReturnZero) {
	EXPECT_EQ(GetTypeSize(DataTypeId::kVertex), 0u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kEdge), 0u);
	EXPECT_EQ(GetTypeSize(DataTypeId::kVarchar), 0u);
}
