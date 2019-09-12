FILE(GLOB srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.cc)

ENABLE_TESTING()
# Build each source file independently
INCLUDE_DIRECTORIES(../)

# gtest
EXECUTE_PROCESS(COMMAND wget https://github.com/google/googletest/archive/release-1.8.1.tar.gz WORKING_DIRECTORY tests)
EXECUTE_PROCESS(COMMAND tar xf release-1.8.1.tar.gz WORKING_DIRECTORY tests)
EXECUTE_PROCESS(COMMAND mv googletest-release-1.8.1/googletest gtest WORKING_DIRECTORY tests)
EXECUTE_PROCESS(COMMAND rm -rf googletest-release-1.8.1 WORKING_DIRECTORY tests)
EXECUTE_PROCESS(COMMAND rm -rf release-1.8.1.tar.gz WORKING_DIRECTORY tests)
EXECUTE_PROCESS(COMMAND cmake . WORKING_DIRECTORY tests/gtest)
EXECUTE_PROCESS(COMMAND make WORKING_DIRECTORY tests/gtest)

INCLUDE_DIRECTORIES(tests/gtest/include)

LINK_DIRECTORIES(
    ../
    gtest/
)

FOREACH(source ${srcs})
    GET_FILENAME_COMPONENT(name ${source} NAME_WE)

    # target
    ADD_EXECUTABLE(${name} ${source})
    TARGET_LINK_LIBRARIES(${name} gamma gtest_main gtest)

    # Install
    INSTALL(TARGETS ${name} DESTINATION test)
ENDFOREACH(source)
