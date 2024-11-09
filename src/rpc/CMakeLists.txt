set(RPC_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/log_entry.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/transport.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erpc_transport.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/rpc_factory.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/rpc_token.cc)

target_sources(dursvr PRIVATE ${RPC_SRC})
target_sources(conssvr PRIVATE ${RPC_SRC})
target_sources(lazylogcli PRIVATE ${RPC_SRC})
target_sources(backendcli PRIVATE ${RPC_SRC})
target_sources(shardsvr PRIVATE ${RPC_SRC})
target_sources(shardsvr_wo_opt PRIVATE ${RPC_SRC})
