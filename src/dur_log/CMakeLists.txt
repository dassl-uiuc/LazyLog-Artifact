set(DUR_LOG_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_log.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_log_flat.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_log_mock.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_svr.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erpc_dl_transport.cc)

add_executable(dursvr ${DUR_LOG_SRC})
add_executable(basic_flat_cli basic_flat_cli.cc)

target_link_libraries(dursvr
    absl::flat_hash_map
    infinity
    ${ERPC_DIR}/build/liberpc.a
    ibverbs
    numa
    glog::glog
    lazylogcli)
target_compile_definitions(dursvr PRIVATE ERPC_INFINIBAND=true)
target_include_directories(dursvr PUBLIC ${CMAKE_SOURCE_DIR}/RDMA/release/include)

target_link_libraries(basic_flat_cli
    infinity
    lazylogcli)
target_compile_definitions(basic_flat_cli PRIVATE ERPC_INFINIBAND=true)
target_include_directories(basic_flat_cli PUBLIC ${CMAKE_SOURCE_DIR}/RDMA/release/include)
