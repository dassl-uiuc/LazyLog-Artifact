add_executable(read_bench read_bench.cc)
add_executable(mixed_bench mixed_bench.cc)
add_executable(append_bench append_bench.cc)

target_link_libraries(mixed_bench
    lazylogcli
    hdr_histogram
    backendcli
    ibverbs)

target_link_libraries(read_bench
    lazylogcli
    hdr_histogram
    backendcli
    ibverbs)

target_link_libraries(append_bench
    lazylogcli
    hdr_histogram
    backendcli
    ibverbs)

target_compile_definitions(read_bench PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(mixed_bench PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(append_bench PRIVATE ERPC_INFINIBAND=true)

