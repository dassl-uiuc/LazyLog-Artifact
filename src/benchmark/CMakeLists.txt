include(FetchContent)

FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG 76dc6715734295ff1866bfc32872ff2278258fc8
)

FetchContent_GetProperties(yaml-cpp)

if(NOT yaml-cpp_POPULATED)
  message(STATUS "Fetching yaml-cpp...")
  FetchContent_Populate(yaml-cpp)
  add_subdirectory(${yaml-cpp_SOURCE_DIR} ${yaml-cpp_BINARY_DIR})
endif()

add_executable(benchmark
  src/main.cpp
  src/workload.cpp
  src/commons.cpp)

target_include_directories(benchmark PRIVATE include)
target_include_directories(benchmark PRIVATE include/producer_consumer)
target_include_directories(benchmark PRIVATE ${CMAKE_SOURCE_DIR}/src)

target_sources(benchmark PRIVATE
  src/producer_consumer/producer.cpp
  src/producer_consumer/consumer.cpp
)

# target_sources(benchmark PRIVATE
#   src/producer_consumer/local/producer_local.cpp
#   src/producer_consumer/local/consumer_local.cpp
#   src/producer_consumer/local/spsc.cpp
# )

target_sources(benchmark PRIVATE
  src/producer_consumer/lazylog/producer_lazylog.cpp
  src/producer_consumer/lazylog/consumer_lazylog.cpp
  src/producer_consumer/lazylog/pc_lazylog.cpp
)

target_link_libraries(benchmark 
  PRIVATE yaml-cpp::yaml-cpp
  PRIVATE hdr_histogram
  PRIVATE lazylogcli
  PRIVATE backendcli
)

target_compile_definitions(benchmark PRIVATE ERPC_INFINIBAND=true)