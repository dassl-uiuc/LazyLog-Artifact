set(CONS_LOG_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/cons_log.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cons_svr.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erpc_cl_transport.cc)

add_executable(conssvr ${CONS_LOG_SRC})

target_link_libraries(conssvr
    ${ERPC_DIR}/build/liberpc.a
    ibverbs
    numa
    glog::glog
    lazylogcli
    backendcli)
target_compile_definitions(conssvr PRIVATE ERPC_INFINIBAND=true)
target_include_directories(conssvr PUBLIC ${CMAKE_SOURCE_DIR}/RDMA/release/include)

add_subdirectory(storage)
