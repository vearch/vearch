SET(ZFP_INCLUDE_SEARCH_PATHS
   /usr/include
   /usr/include/zfp
   /usr/local/include
   /usr/local/include/zfp
   $ENV{ZFP_HOME}
   $ENV{ZFP_HOME}/include
)

SET(ZFP_LIB_SEARCH_PATHS
    /lib/
    /lib64/
    /usr/lib
    /usr/lib64
    /usr/local/lib
    /usr/local/lib64
    $ENV{ZFP_HOME}
    $ENV{ZFP_HOME}/lib
    $ENV{ZFP_HOME}/lib64
 )

FIND_PATH(ZFP_INCLUDE_DIR NAMES zfp.h PATHS ${ZFP_INCLUDE_SEARCH_PATHS})
FIND_LIBRARY(ZFP_LIB NAMES zfp PATHS ${ZFP_LIB_SEARCH_PATHS})

SET(ZFP_FOUND ON)

# Check include files
IF(NOT ZFP_INCLUDE_DIR)
  SET(ZFP_FOUND OFF)
  MESSAGE(STATUS "Could not find ZFP include. Turning ZFP_FOUND off")
ENDIF()

# Check libraries
IF(NOT ZFP_LIB)
  SET(ZFP_FOUND OFF)
  MESSAGE(STATUS "Could not find ZFP lib. Turning ZFP_FOUND off")
ENDIF()

IF (ZFP_FOUND)
  IF (NOT ZFP_FIND_QUIETLY)
    MESSAGE(STATUS "Found ZFP libraries: ${ZFP_LIB}")
    MESSAGE(STATUS "Found ZFP include: ${ZFP_INCLUDE_DIR}")
  ENDIF (NOT ZFP_FIND_QUIETLY)
ELSE (ZFP_FOUND)
  IF (ZFP_FIND_REQUIRED)
    MESSAGE(FATAL_ERROR "Could not find ZFP, please install zfp or set $ZFP_HOME")
  ENDIF (ZFP_FIND_REQUIRED)
ENDIF (ZFP_FOUND)

MARK_AS_ADVANCED(
  ZFP_INCLUDE_DIR
  ZFP_LIB
  ZFP
)

