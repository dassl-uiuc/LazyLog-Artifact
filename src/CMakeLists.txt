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

option(CORFU "Enable CORFU" OFF)

target_compile_definitions(lazylogcli PRIVATE ERPC_INFINIBAND=true)
target_include_directories(lazylogcli PRIVATE ${CMAKE_SOURCE_DIR}/RDMA/release/include)
if(CORFU)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCORFU")
endif(CORFU)

add_subdirectory(client)
add_subdirectory(dur_log)
add_subdirectory(cons_log)
add_subdirectory(rpc)
add_subdirectory(utils)
add_subdirectory(benchmark)
add_subdirectory(app)

add_custom_target(gen_net_config ALL
    DEPENDS ${CMAKE_SOURCE_DIR}/cfg/rdma.prop)
add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/cfg/rdma.prop
    COMMAND python ${CMAKE_SOURCE_DIR}/gen_rdma_config.py ${CMAKE_SOURCE_DIR}/cfg/rdma.prop
    DEPENDS ${CMAKE_SOURCE_DIR}/gen_rdma_config.py)
add_custom_command(
    TARGET gen_net_config
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/cfg/rdma.prop
        ${CMAKE_SOURCE_DIR}/cfg_3_way/rdma.prop)