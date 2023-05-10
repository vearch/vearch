#--------------------------------------------
#    GAMMA ENGINE 
#--------------------------------------------
project(gamma_engine C CXX)
cmake_minimum_required(VERSION 3.17)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

option(BUILD_TEST "Build tests" off)
option(BUILD_WITH_GPU "Build gamma with gpu index support" off)
option(BUILD_WITH_SCANN "Build gamma with scann index support" off)
option(BUILD_TOOLS "Build tools" off)
option(BUILD_FAISSLIKE_INDEX "Build faisslike index" off)
option(BUILD_RELAYOUT "Build relayout index" off)
option(ENABLE_COVERAGE "Use gcov" off)
option(BUILD_PYTHON_SDK "Build python sdk" off)


exec_program(
    "sh"
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party
    ARGS "build.sh" ${BUILD_WITH_GPU})

#ENV VARs
set(THIRDPARTY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(BTREE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/btree)
set(GAMMA_C_API ${CMAKE_CURRENT_SOURCE_DIR}/c_api)

if(APPLE)
    set(CMAKE_C_COMPILER "/usr/local/opt/llvm/bin/clang")
    set(CMAKE_CXX_COMPILER "/usr/local/opt/llvm/bin/clang++")
    include_directories("/usr/local/include"
                        "/usr/local/opt/llvm/include" 
                        "/usr/local/Cellar/openssl@1.1/1.1.1k/include"
                        "/usr/local/opt/openblas/include")
    link_directories("/usr/local/lib"
                     "/usr/local/opt/llvm/lib"
                     "/usr/local/Cellar/openssl@1.1/1.1.1k/lib/"
                     "/usr/local/opt/openblas/lib")
endif(APPLE)

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -std=c++11 -mavx2 -msse4 -mpopcnt -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -DOPEN_CORE -O0 -w -g3 -gdwarf-2")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -std=c++11 -fPIC -m64 -Wall -O3 -mavx2 -msse4 -mpopcnt -fopenmp -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -Werror=narrowing -Wno-deprecated")

if(DEFINED ENV{ROCKSDB_HOME})
    message(STATUS "RocksDB home is set=$ENV{ROCKSDB_HOME}")
    add_definitions(-DWITH_ROCKSDB)
    find_package(RocksDB REQUIRED)
else(DEFINED ENV{ROCKSDB_HOME})
    message(STATUS "RocksDB home isn't set, so store_type=RocksDB is not supported! ")
endif(DEFINED ENV{ROCKSDB_HOME})

if(DEFINED ENV{ZFP_HOME})
    MESSAGE(STATUS "ZFP home is set=$ENV{ZFP_HOME}")
    ADD_DEFINITIONS(-DWITH_ZFP)
    FIND_PACKAGE(ZFP REQUIRED)
else(DEFINED ENV{ZFP_HOME})
    MESSAGE(STATUS "ZFP home isn't set, so COMPRESS is not supported! ")
endif(DEFINED ENV{ZFP_HOME})

#INCLUDE DIR
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${THIRDPARTY}
    ${BTREE}
    ${THIRDPARTY}/faiss/include
    ${RocksDB_INCLUDE_DIR}
    ${ZFP_INCLUDE_DIR}
)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/faiss/lib64")
    link_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/faiss/lib64
        ${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout
    )
else()
    link_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/faiss/lib
        ${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout
    )
endif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/faiss/lib64")

find_package(MKL)
if(MKL_FOUND)
    #LINK LIBRARY
    set(LIBRARIES
        -lfaiss_avx2
        ${RocksDB_LIB}
        ${ZFP_LIB}
        -lcrypto
        -lzstd
        ${MKL_LIBRARIES}
        -ltbb
    )
else()
    set(LIBRARIES
        -lfaiss_avx2
        ${RocksDB_LIB}
        ${ZFP_LIB}
        -lcrypto
        -lzstd
        -lopenblas
        -ltbb
   ) 
endif()

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
aux_source_directory(${THIRDPARTY} DIR_SRCS)
aux_source_directory(${BTREE} DIR_SRCS)
aux_source_directory(${GAMMA_C_API} DIR_SRCS)
aux_source_directory(${GAMMA_C_API}/api_data DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/third_party/cjson DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/third_party/easyloggingpp DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/util DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/common DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/search DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/index DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/storage DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/storage/compress DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/index/impl DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/index/impl/hnswlib DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/index/impl/scann DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/vector DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/realtime DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/table DIR_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/io DIR_SRCS)

add_definitions(-DHAVE_NETINET_IN_H)
add_definitions(-DFINTEGER=int)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Debug Mode")
    message(STATUS "Flags:" ${CMAKE_CXX_FLAGS_DEBUG})
    add_definitions(-DDEBUG_) 
else(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "build type" FORCE)
    message(STATUS "Release Mode")
    message(STATUS "Flags:" ${CMAKE_CXX_FLAGS_RELEASE})
endif(CMAKE_BUILD_TYPE STREQUAL "Debug")

exec_program(
    "git"
    ${CMAKE_CURRENT_SOURCE_DIR}
    ARGS "log --format='[sha1]:%h [author]:%cn [time]:%ci [branch]:%d' -1"
    OUTPUT_VARIABLE VERSION_SHA1)

add_definitions(-DGIT_SHA1="${VERSION_SHA1}")

exec_program(
    "sh"
    ${CMAKE_CURRENT_SOURCE_DIR}/idl
    ARGS "build.sh")

if(PERFORMANCE_TESTING STREQUAL "ON")
    message(STATUS "With preformance testing")
    add_definitions(-DPERFORMANCE_TESTING) 
endif(PERFORMANCE_TESTING STREQUAL "ON")

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout/CMakeLists.txt")
    if(NOT APPLE)
        add_definitions(-DOPT_IVFPQ_RELAYOUT)
        add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout)
        include_directories(${RELAYOUT_INCLUDE})
    endif(NOT APPLE)
endif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/index/impl/relayout/CMakeLists.txt")

set(LIB_SRCS ${DIR_SRCS} ${RELAYOUT_SRCS})

if(BUILD_WITH_GPU)
    message(STATUS "With GPU")
    add_definitions(-DBUILD_WITH_GPU) 
    set(GAMMA_GPU ${CMAKE_CURRENT_SOURCE_DIR}/index/impl/gpu)

    aux_source_directory(${GAMMA_GPU} DIR_SRCS)

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
    -lcublas 
    /usr/local/cuda/lib64/libcudart.so
    )

else(BUILD_WITH_GPU)
    add_library(gamma SHARED ${LIB_SRCS})
    target_link_libraries(gamma ${LIBRARIES})

    if(BUILD_RELAYOUT)
        target_link_libraries(gamma -lrelayout)
    endif()
endif(BUILD_WITH_GPU)

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
endif(BUILD_WITH_SCANN)

if(BUILD_PYTHON_SDK)
    message(STATUS "FOR PYTHON SDK BUILD")
    add_definitions(-DPYTHON_SDK)
endif(BUILD_PYTHON_SDK)


install(TARGETS gamma
	LIBRARY	DESTINATION	lib 
	ARCHIVE	DESTINATION	lib)

install(FILES ${GAMMA_C_API}/gamma_api.h DESTINATION include/gamma)
# install(DIRECTORY ${PROJECT_SOURCE_DIR} DESTINATION include/gamma FILES_MATCHING PATTERN "*.h" PATTERN "build/*" EXCLUDE)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/index DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/common DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/realtime DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/search DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/io DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/table DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/util DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/storage DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/vector DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/c_api DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/idl/fbs-gen/c DESTINATION include/gamma/idl/fbs-gen FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/index/impl/relayout DESTINATION include/gamma FILES_MATCHING PATTERN "*.h")
install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party DESTINATION include/gamma FILES_MATCHING PATTERN "*.h" PATTERN "*.hh")

if(BUILD_TEST)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tests)
endif(BUILD_TEST)

if(BUILD_TOOLS)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/tools)
endif(BUILD_TOOLS)
