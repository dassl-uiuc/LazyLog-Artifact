#!/bin/bash

set -x

threeway="false"
scalable_tput="false"
cfg_dir="cfg"

source $(dirname $0)/common.sh

num_shards=(1 3 5)
msg_size=(4096)
clients=(4 4 8)
cli_idx=0

for size in "${msg_size[@]}";
do
    for shard in "${num_shards[@]}";
    do
        c=${clients[$cli_idx]}
        
        echo "Running for $size msg size, $shard shards, $c clients"

        kill_cons_svr
        kill_shard_svrs
        kill_dur_svrs
        kill_clients

        setup_data
        change_num_shards $shard
        change_stripe_unit 1000
        run_shard_svr $shard
        run_dur_svrs
        run_cons_svr

        run_append_bench 120 $c $size 0
        kill_cons_svr
        kill_shard_svrs
        kill_dur_svrs
        collect_logs
        mkdir -p ${ll_dir}/logs_${c}_${size}_${shard}
        mv $ll_dir/logs/* ${ll_dir}/logs_${c}_${size}_${shard}
        rm -rf $ll_dir/logs
        cli_idx=$((cli_idx+1))
    done
done
