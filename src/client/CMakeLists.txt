target_sources(lazylogcli PRIVATE lazylog_cli.cc)
add_executable(basic_cli basic_client.cc)

target_link_libraries(basic_cli
    lazylogcli
    backendcli
    ibverbs)

target_compile_definitions(lazylogcli PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(basic_cli PRIVATE ERPC_INFINIBAND=true)

add_subdirectory(benchmarking)
