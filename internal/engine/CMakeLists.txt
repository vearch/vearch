cmake_minimum_required(VERSION 3.17)
project(vearch_engine C CXX)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

option(BUILD_TEST "Build tests" off)
option(BUILD_WITH_GPU "Build gamma with gpu index support" off)
option(BUILD_WITH_SCANN "Build gamma with scann index support" off)
option(BUILD_TOOLS "Build tools" off)
option(BUILD_FAISSLIKE_INDEX "Build faisslike index" off)
option(BUILD_RELAYOUT "Build relayout index" off)
option(ENABLE_COVERAGE "Use gcov" off)
option(BUILD_PYTHON_SDK "Build python sdk" off)
# Valid values are "generic", "avx2", "avx512"
option(BUILD_GAMMA_OPT_LEVEL "Build opt level" "avx2")

#ENV VARs
set(THIRDPARTY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(GAMMA_C_API ${CMAKE_CURRENT_SOURCE_DIR}/c_api)

if(APPLE)
    set(CMAKE_C_COMPILER "/usr/local/opt/llvm/bin/clang")
    set(CMAKE_CXX_COMPILER "/usr/local/opt/llvm/bin/clang++")
    include_directories("/usr/local/include"
                        "/usr/local/opt/llvm/include" 
                        "/usr/local/opt/openblas/include")
    link_directories("/usr/local/lib"
                     "/usr/local/opt/llvm/lib"
                     "/usr/local/opt/openblas/lib")
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM|aarch64|AARCH64")
    set(BUILD_GAMMA_OPT_LEVEL "generic")
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM|aarch64|AARCH64")
    set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -std=c++17 -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -DOPEN_CORE -O0 -w -g3 -gdwarf-2")
    set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -std=c++17 -fPIC -Wall -O3 -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -Werror=narrowing -Wno-deprecated")
else()
    set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -std=c++17 -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -DOPEN_CORE -O0 -w -g3 -gdwarf-2")
    set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -std=c++17 -fPIC -m64 -Wall -O3 -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -Werror=narrowing -Wno-deprecated")
endif()

#INCLUDE DIR
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${THIRDPARTY}
)

link_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout
)

#ADDITIONAL SOURCE DIR
file(GLOB_RECURSE ALL_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
     "${CMAKE_CURRENT_SOURCE_DIR}/*.c"
)
list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/third_party/scann-1.2.1/.*")
list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/third_party/flatbuffers-*")
list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/tests/.*")
list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/tools/.*")

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout/CMakeLists.txt")
    if(NOT APPLE)
        add_definitions(-DOPT_IVFPQ_RELAYOUT)
        add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout)
        include_directories(${RELAYOUT_INCLUDE})
    endif()
endif()

if(NOT BUILD_WITH_GPU)
    list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/index/impl/gpu/.*")
endif()

set(LIB_SRCS ${ALL_SOURCES} ${RELAYOUT_SRCS})

if(NOT BUILD_WITH_GPU)
    add_library(gamma SHARED ${LIB_SRCS})

    add_library(gamma_avx2 SHARED ${LIB_SRCS})
    if(NOT BUILD_GAMMA_OPT_LEVEL STREQUAL "avx2" AND NOT BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
        set_target_properties(gamma_avx2 PROPERTIES EXCLUDE_FROM_ALL TRUE)
    endif()
    if(NOT WIN32)
        target_compile_options(gamma_avx2 PRIVATE $<$<OR:$<COMPILE_LANGUAGE:CXX>,$<COMPILE_LANGUAGE:C>>:-mavx2 -mfma -mf16c -mpopcnt>)
    else()
        target_compile_options(gamma_avx2 PRIVATE $<$<OR:$<COMPILE_LANGUAGE:CXX>,$<COMPILE_LANGUAGE:C>>:/arch:AVX2>)
        add_compile_options(/bigobj)
    endif()

    add_library(gamma_avx512 SHARED ${LIB_SRCS})
    if(NOT BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
        set_target_properties(gamma_avx512 PROPERTIES EXCLUDE_FROM_ALL TRUE)
    endif()
    if(NOT WIN32)
        target_compile_options(gamma_avx512 PRIVATE $<$<OR:$<COMPILE_LANGUAGE:CXX>,$<COMPILE_LANGUAGE:C>>:-mavx2 -mfma -mf16c -mavx512f -mavx512cd -mavx512vl -mavx512dq -mavx512bw -mpopcnt>)
    else()
        target_compile_options(gamma_avx512 PRIVATE $<$<OR:$<COMPILE_LANGUAGE:CXX>,$<COMPILE_LANGUAGE:C>>:/arch:AVX512>)
        add_compile_options(/bigobj)
    endif()
endif()

set(COMMON_LIBRARIES
    -lrocksdb
    -ltbb
    -lzstd
    -lprotobuf
    -lroaring
)

set(COMMON_LIBRARIES_AVX2
    -lrocksdb
    -ltbb
    -lzstd
    -lprotobuf
    -lroaring
)

set(COMMON_LIBRARIES_AVX512
    -lrocksdb
    -ltbb
    -lzstd
    -lprotobuf
    -lroaring
)

find_package(MKL)

if (MKL_FOUND)
    if(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
        list(APPEND COMMON_LIBRARIES_AVX512 ${MKL_LIBRARIES})
        list(APPEND COMMON_LIBRARIES_AVX2 ${MKL_LIBRARIES})
        list(APPEND COMMON_LIBRARIES ${MKL_LIBRARIES})
    elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx2")
        list(APPEND COMMON_LIBRARIES_AVX2 ${MKL_LIBRARIES})
        list(APPEND COMMON_LIBRARIES ${MKL_LIBRARIES})
    elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "generic")
        list(APPEND COMMON_LIBRARIES ${MKL_LIBRARIES})
    endif()
else()
    if(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
        list(APPEND COMMON_LIBRARIES_AVX512 -lopenblas)
        list(APPEND COMMON_LIBRARIES_AVX2 -lopenblas)
        list(APPEND COMMON_LIBRARIES -lopenblas)
    elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx2")
        list(APPEND COMMON_LIBRARIES_AVX2 -lopenblas)
        list(APPEND COMMON_LIBRARIES -lopenblas)
    elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "generic")
        list(APPEND COMMON_LIBRARIES -lopenblas)
    endif()
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM|aarch64|AARCH64")
    list(APPEND COMMON_LIBRARIES -lfaiss)
else()
    if(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
        list(APPEND COMMON_LIBRARIES_AVX512 -lfaiss_avx512)
        list(APPEND COMMON_LIBRARIES_AVX2 -lfaiss_avx2)
        list(APPEND COMMON_LIBRARIES -lfaiss)
    elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx2")
        list(APPEND COMMON_LIBRARIES_AVX2 -lfaiss_avx2)
        list(APPEND COMMON_LIBRARIES -lfaiss)
    elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "generic")
        list(APPEND COMMON_LIBRARIES -lfaiss)
    endif()
endif()

set(LIBRARIES ${COMMON_LIBRARIES})

message(STATUS ENABLE_COVERAGE=${ENABLE_COVERAGE})
if(ENABLE_COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

if(BUILD_FAISSLIKE_INDEX)
    add_definitions(-DFAISSLIKE_INDEX)
    MESSAGE(STATUS "BUILD_FAISSLIKE_INDEX=ON")
endif(BUILD_FAISSLIKE_INDEX)

add_definitions(-DHAVE_NETINET_IN_H)
add_definitions(-DFINTEGER=int)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Debug Mode")
    message(STATUS "Flags:" ${CMAKE_CXX_FLAGS_DEBUG})
    add_definitions(-DDEBUG_) 
else()
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
    message(STATUS "Release Mode")
    message(STATUS "Flags:" ${CMAKE_CXX_FLAGS_RELEASE})
endif()

exec_program(
    "git"
    ${CMAKE_CURRENT_SOURCE_DIR}
    ARGS "log --format='[sha1]:%h [author]:%cn [time]:%ci [branch]:%d' -1"
    OUTPUT_VARIABLE VERSION_SHA1)

add_definitions(-DGIT_SHA1="${VERSION_SHA1}")

if(PERFORMANCE_TESTING STREQUAL "ON")
    message(STATUS "With preformance testing")
    add_definitions(-DPERFORMANCE_TESTING) 
endif()

if(BUILD_PYTHON_SDK)
    message(STATUS "FOR PYTHON SDK BUILD")
    add_definitions(-DPYTHON_SDK)
endif()

if(BUILD_WITH_GPU)
    message(STATUS "With GPU (modern CUDA)")
    add_definitions(-DBUILD_WITH_GPU)
    set(GAMMA_GPU ${CMAKE_CURRENT_SOURCE_DIR}/index/impl/gpu)

    include(cmake/Modules/gpu/CUDA.cmake) # may flip BUILD_WITH_GPU OFF if toolkit missing
    if(NOT BUILD_WITH_GPU)
        message(STATUS "Falling back to CPU build because CUDA toolkit was not found")
    endif()
endif()

if(BUILD_WITH_GPU)
    file(GLOB_RECURSE GAMMA_GPU_SOURCES
        ${GAMMA_GPU}/*.h
        ${GAMMA_GPU}/*.hpp
        ${GAMMA_GPU}/*.cuh
        ${GAMMA_GPU}/*.cu
        ${GAMMA_GPU}/*.cpp
        ${GAMMA_GPU}/*.cc)

    # Remove any GPU .cc already present in LIB_SRCS to avoid duplication
    list(FILTER LIB_SRCS EXCLUDE REGEX "${GAMMA_GPU}/.*\\.(cc|cpp|cu)$")

    add_library(gamma SHARED ${LIB_SRCS} ${GAMMA_GPU_SOURCES})

    target_include_directories(gamma
        PRIVATE
            ${GAMMA_GPU}
            ${FAISS_INCLUDE_DIR}
    )

    # Apply CUDA compile options only to CUDA language sources
    foreach(opt IN LISTS VEARCH_CUDA_COMPILE_OPTIONS)
        target_compile_options(gamma PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:${opt}>)
    endforeach()

    # Link imported CUDA toolkit targets (cudart always; cublas/curand if available)
    if(TARGET CUDA::cublas)
        set(_VEARCH_CUDA_LIBS CUDA::cublas)
    endif()
    if(TARGET CUDA::curand)
        list(APPEND _VEARCH_CUDA_LIBS CUDA::curand)
    endif()
    list(APPEND _VEARCH_CUDA_LIBS CUDA::cudart)

    target_link_libraries(gamma
        PRIVATE
            -lfaiss_gpu
            ${LIBRARIES}
            ${_VEARCH_CUDA_LIBS}
            -lbz2
    )

else()
    message(STATUS "With Generic" ${LIBRARIES})
    target_link_libraries(gamma ${LIBRARIES})
    if(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
        message(STATUS "With AVX512" ${COMMON_LIBRARIES_AVX512})
        target_link_libraries(gamma_avx512 ${COMMON_LIBRARIES_AVX512})
        message(STATUS "With AVX2" ${COMMON_LIBRARIES_AVX2})
        target_link_libraries(gamma_avx2 ${COMMON_LIBRARIES_AVX2})
    elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx2")
        message(STATUS "With AVX2" ${COMMON_LIBRARIES_AVX2})
        target_link_libraries(gamma_avx2 ${COMMON_LIBRARIES_AVX2})
    endif()
    if(BUILD_RELAYOUT)
        target_link_libraries(gamma -lrelayout)
        if(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
            target_link_libraries(gamma_avx512 -lrelayout)
            target_link_libraries(gamma_avx2 -lrelayout)
        elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx2")
            target_link_libraries(gamma_avx2 -lrelayout)
        endif()
    endif()
endif()

if(BUILD_WITH_SCANN)
    message(STATUS "With SCANN INDEX")
    exec_program(
        "bash"
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party
        ARGS "build-scann.sh"
        RETURN_VALUE EXEC_RET_VAL)
    if(NOT ${EXEC_RET_VAL} MATCHES "0")
        message(STATUS "SCANN BUILD ERROR, EXEC_RET_VAL=${EXEC_RET_VAL}")
        EXIT()
    endif(NOT ${EXEC_RET_VAL} MATCHES "0")
    add_definitions(-DUSE_SCANN) 
    target_link_libraries(gamma
        ${LIBRARIES}
        -lscannapi
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/scann/lib/libscannapi.so)
    if(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
        target_link_libraries(gamma_avx512 ${COMMON_LIBRARIES_AVX512} -lscannapi
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/scann/lib/libscannapi.so)
        target_link_libraries(gamma_avx2 ${COMMON_LIBRARIES_AVX2} -lscannapi
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/scann/lib/libscannapi.so)
    elseif(BUILD_GAMMA_OPT_LEVEL STREQUAL "avx2")
        target_link_libraries(gamma_avx2 ${COMMON_LIBRARIES_AVX2} -lscannapi
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/scann/lib/libscannapi.so)
    endif()
endif()

install(TARGETS gamma
    EXPORT gamma-targets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
if(NOT BUILD_WITH_GPU AND BUILD_GAMMA_OPT_LEVEL STREQUAL "avx2")
    install(TARGETS gamma_avx2
        EXPORT gamma-targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
if(NOT BUILD_WITH_GPU AND BUILD_GAMMA_OPT_LEVEL STREQUAL "avx512")
    install(TARGETS gamma_avx2 gamma_avx512
        EXPORT gamma-targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()

install(FILES ${GAMMA_C_API}/gamma_api.h DESTINATION include/gamma)
set(INSTALL_INCLUDE_DIRS
    index
    common
    index/realtime
    search
    io
    table
    util
    storage
    vector
    c_api
    idl/fbs-gen/c
    index/impl/relayout
    third_party
)

foreach(dir ${INSTALL_INCLUDE_DIRS})
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/${dir} 
            DESTINATION include/gamma 
            FILES_MATCHING PATTERN "*.h" 
            PATTERN "build/*" EXCLUDE)
endforeach()

if(BUILD_TEST)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
endif()

if(BUILD_TOOLS)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tools)
endif()
