SET(RocksDB_INCLUDE_SEARCH_PATHS
   /usr/include
   /usr/include/faiss
   /usr/local/include
   /usr/local/include/faiss
   $ENV{ROCKSDB_HOME}
   $ENV{ROCKSDB_HOME}/include
)

SET(RocksDB_LIB_SEARCH_PATHS
    /lib/
    /lib64/
    /usr/lib
    /usr/lib64
    /usr/local/lib
    /usr/local/lib64
    $ENV{ROCKSDB_HOME}
    $ENV{ROCKSDB_HOME}/lib
 )

FIND_PATH(RocksDB_INCLUDE_DIR NAMES rocksdb/db.h PATHS ${RocksDB_INCLUDE_SEARCH_PATHS})
FIND_LIBRARY(RocksDB_LIB NAMES rocksdb PATHS ${RocksDB_LIB_SEARCH_PATHS})

SET(RocksDB_FOUND ON)

# Check include files
IF(NOT RocksDB_INCLUDE_DIR)
  SET(RocksDB_FOUND OFF)
  MESSAGE(STATUS "Could not find RocksDB include. Turning RocksDB_FOUND off")
ENDIF()

# Check libraries
IF(NOT RocksDB_LIB)
  SET(RocksDB_FOUND OFF)
  MESSAGE(STATUS "Could not find RocksDB lib. Turning RocksDB_FOUND off")
ENDIF()

IF (RocksDB_FOUND)
  IF (NOT RocksDB_FIND_QUIETLY)
    MESSAGE(STATUS "Found RocksDB libraries: ${RocksDB_LIB}")
    MESSAGE(STATUS "Found RocksDB include: ${RocksDB_INCLUDE_DIR}")
  ENDIF (NOT RocksDB_FIND_QUIETLY)
ELSE (RocksDB_FOUND)
  IF (RocksDB_FIND_REQUIRED)
    MESSAGE(FATAL_ERROR "Could not find RocksDB, please install RocksDB or set $ROCKSDB_HOME")
  ENDIF (RocksDB_FIND_REQUIRED)
ENDIF (RocksDB_FOUND)

MARK_AS_ADVANCED(
  RocksDB_INCLUDE_DIR
  RocksDB_LIB
  RocksDB
)

