cmake_minimum_required(VERSION 3.20)

project(async-test)

set(CMAKE_CXX_STANDARD 17)

set(EXEC_FILES
        ./test_main.cpp
        ./promise_test.cpp
        )

add_executable(async_test ${EXEC_FILES})

find_package(GTest REQUIRED)
message(STATUS "GTEST_INCLUDE_DIRS: ${GTEST_INCLUDE_DIRS}")
message(STATUS "GTEST_BOTH_LIBRARIES: ${GTEST_BOTH_LIBRARIES}")
message(STATUS "GTEST_LIBRARIES: ${GTEST_LIBRARIES}")
message(STATUS "GTEST_MAIN_LIBRARIES: ${GTEST_MAIN_LIBRARIES}")

target_link_libraries(async_test ${GTEST_LIBRARIES})