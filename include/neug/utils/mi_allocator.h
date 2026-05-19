#pragma once

#include <memory>

#ifdef WITH_MIMALLOC
#include <mimalloc.h>
#endif

namespace neug {

template <typename T>
#ifdef WITH_MIMALLOC
using NeuGAllocator = mi_stl_allocator<T>;
#else
using NeuGAllocator = std::allocator<T>;
#endif

}  // namespace neug
