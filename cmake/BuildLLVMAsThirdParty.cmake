# Build LLVM as third party dependency
function(build_llvm_as_third_party)
    # Find LLVM package
    find_package(LLVM REQUIRED CONFIG)
    
    # Set required LLVM components
    set(LLVM_COMPONENTS core support orcjit native)
    
    # Get LLVM include directories
    llvm_map_components_to_libnames(llvm_libs ${LLVM_COMPONENTS})
    
    # Set LLVM include directories
    include_directories(${LLVM_INCLUDE_DIRS})
    
    # Set LLVM library directories
    link_directories(${LLVM_LIBRARY_DIRS})
    
    # Set LLVM libraries variable
    set(LLVM_LIBRARIES ${llvm_libs} PARENT_SCOPE)
    
    message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
    message(STATUS "Using LLVM include directories: ${LLVM_INCLUDE_DIRS}")
    message(STATUS "Using LLVM library directories: ${LLVM_LIBRARY_DIRS}")
    message(STATUS "Using LLVM libraries: ${LLVM_LIBRARIES}")
endfunction()
