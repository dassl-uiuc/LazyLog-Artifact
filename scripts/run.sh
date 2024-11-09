#!/bin/bash

set -x 

threeway="false"
scalable_tput="false"
cfg_dir="cfg"

source $(dirname $0)/common.sh

# mode 
#   0 -> run expt
#   

mode="$1"
clients=("1")
num_shards=("1")
msg_size=("4096")
if [ "$mode" -eq 0 ]; then # run expt   
    for clients in "${clients[@]}";
    do 
        echo "Running for $clients clients"
        kill_shard_svrs
        kill_cons_svr
        kill_dur_svrs
        kill_clients

        run_shard_svr
        run_dur_svrs
        run_cons_svr

        drop_shard_caches
        run_read_bench 10000000 180 $clients
        kill_shard_svrs
        kill_dur_svrs
        kill_cons_svr
        collect_logs
        mkdir -p ${ll_dir}/logs_$clients
        mv $ll_dir/logs/* ${ll_dir}/logs_$clients
    done 
elif [ "$mode" -eq 1 ]; then # load 10 million keys
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs
    kill_clients

    setup_data
    run_shard_svr
    run_dur_svrs
    run_cons_svr

    # load 10 million keys
    load_keys 10000 1000
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs

    collect_logs
elif [ "$mode" -eq 2 ]; then
    for size in "${msg_size[@]}"; 
    do 
        for shard in "${num_shards[@]}";
        do 
            for c in "${clients[@]}";
            do 
                echo "Running for $c clients, $size msg size, $shard shards"
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
            done 
        done
    done 
elif [ "$mode" -eq 3 ]; then
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs
    kill_clients

    setup_data
    run_shard_svr
    run_dur_svrs
    run_cons_svr

    echo $3 $4
    $(mixed_cmd $3 $4)
elif [ "$mode" -eq 4 ]; then 
    kill_cons_svr
    kill_dur_svrs
    kill_shard_svrs

    setup_data
    run_shard_svr 1
    run_dur_svrs
    run_cons_svr
else 
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs
    kill_clients
    collect_logs
fi
