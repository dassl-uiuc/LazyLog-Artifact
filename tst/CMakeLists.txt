add_executable(lazylog_test
    log_entry_test.cc
    flat_log_test.cc
    ${CMAKE_SOURCE_DIR}/src/rpc/log_entry.cc
    ${CMAKE_SOURCE_DIR}/src/dur_log/dur_log_flat.cc
    ${CMAKE_SOURCE_DIR}/src/dur_log/dur_log.cc)

target_link_libraries(lazylog_test
    absl::flat_hash_map
    GTest::gtest_main
    infinity
    lazylogcli
)

target_compile_definitions(lazylog_test PRIVATE ERPC_INFINIBAND=true)
target_include_directories(lazylog_test PUBLIC ${CMAKE_SOURCE_DIR}/RDMA/release/include)

include(GoogleTest)
gtest_discover_tests(lazylog_test)
