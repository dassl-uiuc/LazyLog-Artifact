add_library(lazylogcli STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_log/dur_log_erpc_cli.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cons_log/cons_log_erpc_cli.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/dur_log/dur_log_flat_cli.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/client/lazylog_cli.cc)
target_link_libraries(lazylogcli
    PRIVATE ${ERPC_DIR}/build/liberpc.a
    PRIVATE ibverbs
    PRIVATE numa
    PRIVATE infinity
    PUBLIC glog::glog
)
target_compile_definitions(lazylogcli PRIVATE ERPC_INFINIBAND=true)
target_include_directories(lazylogcli PRIVATE ${CMAKE_SOURCE_DIR}/RDMA/release/include)

add_subdirectory(client)
add_subdirectory(dur_log)
add_subdirectory(cons_log)
add_subdirectory(rpc)
add_subdirectory(utils)

add_custom_target(gen_net_config ALL
    DEPENDS ${CMAKE_SOURCE_DIR}/cfg/rdma.prop)
add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/cfg/rdma.prop
    COMMAND python ${CMAKE_SOURCE_DIR}/gen_rdma_config.py ${CMAKE_SOURCE_DIR}/cfg/rdma.prop
    DEPENDS ${CMAKE_SOURCE_DIR}/gen_rdma_config.py)
