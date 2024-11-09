add_library(backendcli STATIC
    naive_backend.cc
    kafka_backend.cc
    shard_client.cc)

add_executable(shardsvr_wo_opt
    shard_svr_wo_opt.cc
    shard_server_wo_opt.cc
    shard_client.cc)

add_executable(shardsvr
    shard_svr.cc
    shard_server.cc
    shard_client.cc)

add_executable(basic_be basic_backend.cc)
add_executable(basic_be_read basic_backend_read.cc)

target_link_libraries(backendcli
    PRIVATE ${ERPC_DIR}/build/liberpc.a
    PRIVATE ibverbs
    PRIVATE numa
    PRIVATE infinity
    PUBLIC glog::glog
    PUBLIC cppkafka)

target_link_libraries(shardsvr
    ${ERPC_DIR}/build/liberpc.a
    ibverbs
    numa
    glog::glog)

target_link_libraries(shardsvr_wo_opt
    ${ERPC_DIR}/build/liberpc.a
    ibverbs
    numa
    glog::glog)

target_link_libraries(basic_be
    hdr_histogram
    backendcli)

target_link_libraries(basic_be_read
    hdr_histogram
    backendcli) 

target_compile_definitions(backendcli PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(shardsvr PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(shardsvr_wo_opt PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(basic_be PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(basic_be_read PRIVATE ERPC_INFINIBAND=true)