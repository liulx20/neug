#pragma once
#include <cstdint>

namespace neug::execution::vec {

enum class VectorType : uint8_t {
	kFlat,
	kConstant,
	kDictionary,
};

}  // namespace neug::execution::vec
