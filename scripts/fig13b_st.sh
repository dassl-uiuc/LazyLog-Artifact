#!/bin/bash

set -x

threeway="false"
scalable_tput="false"
cfg_dir="cfg"

source $(dirname $0)/common.sh

shard=5
msg_size=4096
clients=(4 8 12 16 18)

for c in "${clients[@]}";
do
    echo "Running for $msg_size msg size, $shard shards, $c clients"

    kill_cons_svr
    kill_shard_svrs
    kill_dur_svrs
    kill_clients

    setup_data
    change_num_shards $shard
    change_stripe_unit 8000
    run_shard_svr $shard
    run_dur_svrs
    run_cons_svr

    run_append_bench 120 $c $msg_size
    kill_cons_svr
    kill_shard_svrs
    kill_dur_svrs
    collect_logs
    mkdir -p ${ll_dir}/logs_${c}_${msg_size}_${shard}_st
    mv $ll_dir/logs/* ${ll_dir}/logs_${c}_${msg_size}_${shard}_st
    rm -rf $ll_dir/logs
done
