#ifdef NEUG_ENABLE_JIT_EXPRESSION

#include "neug/execution/expression/codegen/jit_engine.h"

#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Error.h>
#include <glog/logging.h>
#include <mutex>

namespace neug {
namespace execution {
namespace codegen {

static std::once_flag llvm_init_flag;

static void initializeLLVM() {
  std::call_once(llvm_init_flag, []() {
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
  });
}

JitEngine::JitEngine() {
  initializeLLVM();
  auto expected_jit = llvm::orc::LLJITBuilder().create();
  if (!expected_jit) {
    LOG(FATAL) << "Failed to create LLJIT: "
               << llvm::toString(expected_jit.takeError());
  }
  jit_ = std::move(*expected_jit);
}

JitEngine& JitEngine::instance() {
  static JitEngine engine;
  return engine;
}

void* JitEngine::compile(std::unique_ptr<llvm::LLVMContext> context,
                         std::unique_ptr<llvm::Module> module,
                         const std::string& function_name) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto tsm = llvm::orc::ThreadSafeModule(std::move(module), std::move(context));

  auto err = jit_->addIRModule(std::move(tsm));
  if (err) {
    LOG(ERROR) << "Failed to add IR module: " << llvm::toString(std::move(err));
    return nullptr;
  }

  auto expected_symbol = jit_->lookup(function_name);
  if (!expected_symbol) {
    LOG(ERROR) << "Failed to lookup symbol '" << function_name
               << "': " << llvm::toString(expected_symbol.takeError());
    return nullptr;
  }

  return reinterpret_cast<void*>(expected_symbol->getAddress());
}

}  // namespace codegen
}  // namespace execution
}  // namespace neug

#endif  // NEUG_ENABLE_JIT_EXPRESSION
