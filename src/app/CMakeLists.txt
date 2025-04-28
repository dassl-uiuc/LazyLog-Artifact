add_executable(kvsvr ll_kv.cc)

target_link_libraries(kvsvr
    ${ERPC_DIR}/build/liberpc.a
    ibverbs
    numa
    lazylogcli
    backendcli
    absl::flat_hash_map)

target_compile_definitions(kvsvr PRIVATE ERPC_INFINIBAND=true)
