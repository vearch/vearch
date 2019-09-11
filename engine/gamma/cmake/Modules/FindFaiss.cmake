SET(Faiss_INCLUDE_SEARCH_PATHS
   /usr/include
   /usr/include/faiss
   /usr/local/include
   /usr/local/include/faiss
   $ENV{FAISS_HOME}
   $ENV{FAISS_HOME}/include
)

SET(Faiss_LIB_SEARCH_PATHS
    /lib/
    /lib64/
    /usr/lib
    /usr/lib64
    /usr/local/lib
    /usr/local/lib64
    $ENV{FAISS_HOME}
    $ENV{FAISS_HOME}/lib
 )

FIND_PATH(Faiss_INCLUDE_DIR NAMES faiss/Index.h PATHS ${Faiss_INCLUDE_SEARCH_PATHS})
FIND_LIBRARY(Faiss_LIB NAMES faiss PATHS ${Faiss_LIB_SEARCH_PATHS})

SET(Faiss_FOUND ON)

# Check include files
IF(NOT Faiss_INCLUDE_DIR)
  SET(Faiss_FOUND OFF)
  MESSAGE(STATUS "Could not find Faiss include. Turning Faiss_FOUND off")
ENDIF()

# Check libraries
IF(NOT Faiss_LIB)
  SET(Faiss_FOUND OFF)
  MESSAGE(STATUS "Could not find Faiss lib. Turning Faiss_FOUND off")
ENDIF()

IF (Faiss_FOUND)
  IF (NOT Faiss_FIND_QUIETLY)
    MESSAGE(STATUS "Found Faiss libraries: ${Faiss_LIB}")
    MESSAGE(STATUS "Found Faiss include: ${Faiss_INCLUDE_DIR}")
  ENDIF (NOT Faiss_FIND_QUIETLY)
ELSE (Faiss_FOUND)
  IF (Faiss_FIND_REQUIRED)
    MESSAGE(FATAL_ERROR "Could not find Faiss, please install faiss or set $FAISS_HOME")
  ENDIF (Faiss_FIND_REQUIRED)
ENDIF (Faiss_FOUND)

MARK_AS_ADVANCED(
  Faiss_INCLUDE_DIR
  Faiss_LIB
  Faiss
)

