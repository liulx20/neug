#pragma once
#ifdef NEUG_ENABLE_JIT_EXPRESSION

#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/Error.h>
#include <memory>
#include <mutex>
#include <string>

namespace neug {
namespace execution {
namespace codegen {

class JitEngine {
 public:
  static JitEngine& instance();

  // Compile a module and return the function pointer for the given symbol name.
  // Takes ownership of the context and module.
  // Thread-safe: serializes access to the underlying LLJIT instance.
  void* compile(std::unique_ptr<llvm::LLVMContext> context,
                std::unique_ptr<llvm::Module> module,
                const std::string& function_name);

 private:
  JitEngine();
  std::unique_ptr<llvm::orc::LLJIT> jit_;
  std::mutex mutex_;
};

}  // namespace codegen
}  // namespace execution
}  // namespace neug

#endif  // NEUG_ENABLE_JIT_EXPRESSION
