file(GLOB srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.cc)

enable_testing()
# Build each source file independently
include_directories(../)

# gtest
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/gtest")
    execute_process(COMMAND rm -rf gtest WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    execute_process(COMMAND wget https://github.com/google/googletest/archive/release-1.8.1.tar.gz WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    execute_process(COMMAND tar xf release-1.8.1.tar.gz WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    execute_process(COMMAND mv googletest-release-1.8.1/googletest gtest WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    execute_process(COMMAND rm -rf googletest-release-1.8.1 WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    execute_process(COMMAND rm -rf release-1.8.1.tar.gz WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    execute_process(COMMAND cmake . WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/gtest)
    execute_process(COMMAND make WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/gtest)
endif()

include_directories(gtest/include)

link_directories(
    ../
    gtest/
    ../index/gpu
)

foreach(source ${srcs})
    get_filename_component(name ${source} NAME_WE)

    # target
    add_executable(${name} ${source})
    target_link_libraries(${name} gamma gtest_main gtest)

    # Install
    install(TARGETS ${name} DESTINATION test)
endforeach(source)

# SET(tests_src
#     ${CMAKE_CURRENT_SOURCE_DIR}/test_realtime_mem_data.cc
#     ${CMAKE_CURRENT_SOURCE_DIR}/test_raw_vector.cc)

# ADD_EXECUTABLE(all_unit_tests ${tests_src})

# TARGET_LINK_LIBRARIES(all_unit_tests gamma gtest_main gtest)
# INSTALL(TARGETS all_unit_tests DESTINATION test)

