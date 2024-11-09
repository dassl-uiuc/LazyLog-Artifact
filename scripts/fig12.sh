#!/bin/bash

set -x

threeway="false"
scalable_tput="false"

size=(100 512 1024 4096 8192)
clients=(18 16 16 6 4)
strip_unit=(40000 8000 4000 1000 500)
cfg_dir="cfg"

source $(dirname $0)/common.sh

for i in {0..4}; do
    s=${size[$i]}
    c=${clients[$i]}
    u=${strip_unit[$i]}

    echo "Running for $c clients, $s size"
    kill_cons_svr
    kill_shard_svrs
    kill_dur_svrs
    kill_clients

    setup_data
    change_num_shards 5
    change_stripe_unit $u
    run_shard_svr 5
    run_dur_svrs
    run_cons_svr

    run_append_bench 120 $c $s 0
    kill_cons_svr
    kill_shard_svrs
    kill_dur_svrs
    collect_logs
    mkdir -p ${ll_dir}/logs_${c}_${s}
    mv ${ll_dir}/logs/* ${ll_dir}/logs_${c}_${s}
    rm -rf ${ll_dir}/logs
done
