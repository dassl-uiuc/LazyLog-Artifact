add_executable(datalogsvr
    datalog.cc
    datalog_svr.cc)

target_link_libraries(datalogsvr
    backendcli
    ${ERPC_DIR}/build/liberpc.a
    ibverbs
    numa
    glog::glog)

target_compile_definitions(datalogsvr PRIVATE ERPC_INFINIBAND=true)

