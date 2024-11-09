set(UTILS_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/properties.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/sequencer.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/timer.cc)

target_sources(dursvr PRIVATE ${UTILS_SRC})
target_sources(conssvr PRIVATE ${UTILS_SRC})
target_sources(lazylogcli PRIVATE ${UTILS_SRC})
target_sources(backendcli PRIVATE ${UTILS_SRC})
target_sources(shardsvr PRIVATE ${UTILS_SRC})
target_sources(shardsvr_wo_opt PRIVATE ${UTILS_SRC})
