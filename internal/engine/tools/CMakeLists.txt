FILE(GLOB srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.cc)

ENABLE_TESTING()
# Build each source file independently
INCLUDE_DIRECTORIES(
  ../
  ../tests
)

LINK_DIRECTORIES(
    ../
)

FOREACH(source ${srcs})
    GET_FILENAME_COMPONENT(name ${source} NAME_WE)

    # target
    ADD_EXECUTABLE(${name} ${source})
    TARGET_LINK_LIBRARIES(${name} gamma)

    # Install
    INSTALL(TARGETS ${name} DESTINATION test)
ENDFOREACH(source)
