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
option(BUILD_THREAD_NUM "Build faiss thread num" 4)


exec_program(
    "sh"
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party
    ARGS "build.sh" ${BUILD_WITH_GPU} ${BUILD_THREAD_NUM})

#ENV VARs
set(THIRDPARTY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(BTREE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/btree)
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
    set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -std=c++17 -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -DOPEN_CORE -O0 -w -g3 -gdwarf-2")
    set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -std=c++17 -fPIC -Wall -O3 -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -Werror=narrowing -Wno-deprecated")
else()
    set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -std=c++17 -mavx2 -msse4 -mpopcnt -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -DOPEN_CORE -O0 -w -g3 -gdwarf-2")
    set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -std=c++17 -fPIC -m64 -Wall -O3 -mavx2 -msse4 -mpopcnt -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -Werror=narrowing -Wno-deprecated")
endif()

if(NOT DEFINED ENV{ROCKSDB_HOME})
    message(FATAL_ERROR "RocksDB home isn't set! ")
endif()

message(STATUS "RocksDB home is set=$ENV{ROCKSDB_HOME}")
find_package(RocksDB REQUIRED)

#INCLUDE DIR
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${THIRDPARTY}
    ${BTREE}
    ${THIRDPARTY}/faiss/include
    ${RocksDB_INCLUDE_DIR}
)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/faiss/lib64")
    link_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/faiss/lib64
        ${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout
    )
else()
    link_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/faiss/lib
    )
endif()

set(COMMON_LIBRARIES
    ${RocksDB_LIB}
    -ltbb
    -lzstd
    -lprotobuf
)

find_package(MKL)

if (MKL_FOUND)
    list(APPEND COMMON_LIBRARIES ${MKL_LIBRARIES})
else()
    list(APPEND COMMON_LIBRARIES -lopenblas)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|ARM|aarch64|AARCH64")
    list(APPEND COMMON_LIBRARIES -lfaiss)
else()
    list(APPEND COMMON_LIBRARIES -lfaiss_avx2)
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

#ADDITIONAL SOURCE DIR
file(GLOB_RECURSE ALL_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
     "${CMAKE_CURRENT_SOURCE_DIR}/*.c"
)
list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/third_party/scann-1.2.1/.*")
list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/third_party/flatbuffers-*")
list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/tests/.*")
list(FILTER ALL_SOURCES EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/tools/.*")

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

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout/CMakeLists.txt")
    if(NOT APPLE)
        add_definitions(-DOPT_IVFPQ_RELAYOUT)
        add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout)
        include_directories(${RELAYOUT_INCLUDE})
    endif()
endif()

set(LIB_SRCS ${ALL_SOURCES} ${RELAYOUT_SRCS})

if(BUILD_WITH_GPU)
    message(STATUS "With GPU")
    add_definitions(-DBUILD_WITH_GPU) 
    set(GAMMA_GPU ${CMAKE_CURRENT_SOURCE_DIR}/index/impl/gpu)

    aux_source_directory(${GAMMA_GPU} ALL_SOURCES)

    include(cmake/Modules/gpu/CUDA.cmake)
    
    file(GLOB_RECURSE FAISS_GPU_HEADERS ${GAMMA_GPU}/*.h)
    file(GLOB_RECURSE FAISS_GPU_CPP ${GAMMA_GPU}/*.cpp)
    file(GLOB_RECURSE FAISS_GPU_CC ${GAMMA_GPU}/*.cc)
    file(GLOB_RECURSE FAISS_GPU_CUH ${GAMMA_GPU}/*.cuh)
    file(GLOB_RECURSE FAISS_GPU_CU ${GAMMA_GPU}/*.cu)

    include_directories(
        ${GAMMA_GPU}
        ${FAISS_INCLUDE_DIR}
        ${CUDA_INCLUDE_DIRS}
    )

    CUDA_ADD_LIBRARY(gamma SHARED 
    ${FAISS_GPU_HEADERS}
    ${FAISS_GPU_CPP}
    ${FAISS_GPU_CC}
    ${FAISS_GPU_CUH}
    ${FAISS_GPU_CU}
    ${LIB_SRCS})

    target_link_libraries(gamma 
    ${LIBRARIES}
    /usr/local/cuda/lib64/libcublas.so
    /usr/local/cuda/lib64/libcudart.so
    )
else()
    list(FILTER LIB_SRCS EXCLUDE REGEX "${CMAKE_CURRENT_SOURCE_DIR}/index/impl/gpu/.*")
    add_library(gamma SHARED ${LIB_SRCS})
    target_link_libraries(gamma ${LIBRARIES})

    if(BUILD_RELAYOUT)
        target_link_libraries(gamma -lrelayout)
    endif()
endif()

if(BUILD_WITH_SCANN)
    message(STATUS "With SCANN INDEX")
    exec_program(
        "sh"
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
endif()

if(BUILD_PYTHON_SDK)
    message(STATUS "FOR PYTHON SDK BUILD")
    add_definitions(-DPYTHON_SDK)
endif()


install(TARGETS gamma
	LIBRARY	DESTINATION	lib 
	ARCHIVE	DESTINATION	lib)

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
