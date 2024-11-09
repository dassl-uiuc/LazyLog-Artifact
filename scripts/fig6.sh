#!/bin/bash

set -x

threeway="true"
scalable_tput="false"
size=4096
cfg_dir="cfg_3_way"

num_shards=(1 5)
clients=(1 6)
tputs=(30000 150000)

source $(dirname $0)/common.sh

for i in {0..1}; do
    c=${clients[$i]}
    s=${num_shards[$i]}
    t=${tputs[$i]}

    echo "Running for $c clients, $s shards, $t tput"
    kill_cons_svr
    kill_shard_svrs
    kill_dur_svrs
    kill_clients

    setup_data
    change_num_shards $s
    run_shard_svr $s
    run_dur_svrs
    run_cons_svr

    run_append_bench 120 $c $size $t
    kill_cons_svr
    kill_shard_svrs
    kill_dur_svrs
    collect_logs
    mkdir -p ${ll_dir}/logs_${c}_${size}_${s}_${t}
    mv $ll_dir/logs/* ${ll_dir}/logs_${c}_${size}_${s}_${t}
    rm -rf $ll_dir/logs
done
