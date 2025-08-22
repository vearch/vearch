include_guard(GLOBAL)

if(NOT BUILD_WITH_GPU)
    return()
endif()

# Enable CUDA language (idempotent if already enabled)
enable_language(CUDA)

find_package(CUDAToolkit QUIET)
if(NOT CUDAToolkit_FOUND)
    message(WARNING "CUDAToolkit not found; disabling GPU build")
    set(BUILD_WITH_GPU OFF CACHE BOOL "Build gamma with gpu index support" FORCE)
    return()
endif()

# CUDA standard
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_EXTENSIONS OFF)

if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES OR CMAKE_CUDA_ARCHITECTURES STREQUAL "")
    # Default set; adjust according to project support matrix
    set(CMAKE_CUDA_ARCHITECTURES 70;75;80;86 CACHE STRING "CUDA architectures" FORCE)
endif()

# Build-type specific CUDA compile options (target-specific will still be applied in parent)
set(_VEARCH_CUDA_COMMON_OPTS -Xcompiler=-fPIC -Xcompiler=-fopenmp)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(_VEARCH_CUDA_BUILD_OPTS -O0 -g -lineinfo -DDEBUG_)
else()
    set(_VEARCH_CUDA_BUILD_OPTS -O3 -DNDEBUG)
endif()

set(VEARCH_CUDA_COMPILE_OPTIONS ${_VEARCH_CUDA_COMMON_OPTS} ${_VEARCH_CUDA_BUILD_OPTS} CACHE INTERNAL "Internal list of CUDA compile options")

message(STATUS "CUDA toolkit: include dirs=${CUDAToolkit_INCLUDE_DIRS}; libs will use imported targets. Archs=${CMAKE_CUDA_ARCHITECTURES}")
