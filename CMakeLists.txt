cmake_minimum_required(VERSION 3.14)

project(LazyLog)
set(CMAKE_CXX_STANDARD 17)
set(ABSL_PROPAGATE_CXX_STD ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

include(FetchContent)

FetchContent_Declare(
    glog
    GIT_REPOSITORY https://github.com/google/glog.git
    GIT_TAG v0.6.0
)
FetchContent_MakeAvailable(glog)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.12.1
)
FetchContent_MakeAvailable(googletest)

if(NOT ERPC_DIR)
    set(ERPC_DIR ${CMAKE_SOURCE_DIR}/../eRPC)
endif()
message("ERPC dir is ${ERPC_DIR}")

find_package(Threads REQUIRED)

include_directories(
    ${ERPC_DIR}/src
    ${ERPC_DIR}/third_party/asio/include
    ${CMAKE_CURRENT_SOURCE_DIR}/cppkafka/include)

add_subdirectory(abseil-cpp)
add_subdirectory(HdrHistogram_c)
add_subdirectory(RDMA)
add_subdirectory(cppkafka)
add_subdirectory(src)
add_subdirectory(tst)
