#!/bin/bash
set -x
cons_svr="node0"
dur_svrs=("node1")
shard_0=("node7" "node8" "node6") # primary last
shard_1=("node6" "node7" "node8") # primary last
shard_2=("node10" "node11" "node9") # primary last
shard_3=("node9" "node10" "node11") # primary last
shard_4=("node13" "node14" "node12") # primary last

source $(dirname $0)/../usr_cfg.sh
ll_dir=$(realpath $(dirname $0)/../..)

log_dir="$data_dir/logs"
ll_dir="/proj/rasl-PG0/LL-AE/LazyLog-Artifact"
cfg_dir="${ll_dir}/scripts/benchmark/cfg"

# arg: ip_addr of node, number of threads
dur_cmd() {
    echo "sudo GLOG_minloglevel=1 ./build/src/dur_log/dursvr -P ${cfg_dir}/durlog.prop -P ${cfg_dir}/rdma.prop -p dur_log.server_uri=$1:31850 -p threadcount=$2"
}

cons_cmd() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/conssvr -P ${cfg_dir}/conslog.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -P ${cfg_dir}/dl_client.prop"
}

shard_cmd_primary() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/storage/shardsvr -P ${cfg_dir}/be.prop -P ${cfg_dir}/shard$1.prop -P ${cfg_dir}/rdma.prop -p leader=true"
}

# arg: ip_addr of node
shard_cmd_backup() {
    local port=$((2 * $2))
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/storage/shardsvr -P ${cfg_dir}/be.prop -P ${cfg_dir}/shard$2.prop -P ${cfg_dir}/rdma.prop -p shard.server_uri=$1:3186$port"
}

# args: batch size, round
basic_be_cmd() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/storage/basic_be -P ${cfg_dir}/be.prop -P ${cfg_dir}/rdma.prop -p batch=$1 -p round=$2"
}

# args: requests, runtime in secs, threads 
read_cmd() {
    echo "sudo GLOG_minloglevel=1 ./build/src/client/benchmarking/read_bench -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/client.prop -P ${cfg_dir}/be.prop -p request_count=$1 -p runtime_secs=$2 -p threadcount=$3"
}

dur_svrs_ip=()
backup_ip=""

# arg: node to ssh into
get_ip() {
    ip=$(ssh -o StrictHostKeyChecking=no -i $pe $username@$1 "ifconfig | grep 'netmask 255.255.255.0'")
    ip=$(echo $ip | awk '{print $2}')
    echo $ip
}

# arg: number of threads
run_dur_svrs() {
    local primary_done=false
    for svr in "${dur_svrs[@]}"; 
    do 
        if ${primary_done}; then 
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(dur_cmd $(get_ip $svr) $1) > $log_dir/dursvr_$svr.log 2>&1 &\""
        else 
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(dur_cmd $(get_ip $svr) $1) -p leader=true > $log_dir/dursvr_$svr.log 2>&1 &\""
            primary_done=true
        fi 
    done 
}

run_dur_svr_pri() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@${dur_svrs[0]} "sh -c \"cd $ll_dir && nohup $(dur_cmd $(get_ip ${dur_svrs[0]}) $1) -p leader=true > $log_dir/dursvr_${dur_svrs[0]}.log 2>&1 &\""
}

run_cons_svr() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@$cons_svr "sh -c \"cd $ll_dir && nohup $(cons_cmd) > $log_dir/conssvr_$cons_svr.log 2>&1 &\""
}

run_single_shard_svr() {
    for ((idx=0; idx<${#shard_0[@]}; idx++)); do
        svr="${shard_0[idx]}"
        if [ $idx -eq $(( ${#shard_0[@]} - 1 )) ]; then
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_primary 0) > $log_dir/shardsvr_${svr}_0.log 2>&1 &\""
        else
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_backup $(get_ip $svr) 0) > $log_dir/shardsvr_${svr}_0.log 2>&1 &\""
        fi
    done
}

run_shard_svr() {
    for ((idx=0; idx<${#shard_0[@]}; idx++)); do
        svr="${shard_0[idx]}"
        if [ $idx -eq $(( ${#shard_0[@]} - 1 )) ]; then
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_primary 0) > $log_dir/shardsvr_${svr}_0.log 2>&1 &\""
            # sleep 10
        else
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_backup $(get_ip $svr) 0) > $log_dir/shardsvr_${svr}_0.log 2>&1 &\""
        fi
    done

    for ((idx=0; idx<${#shard_1[@]}; idx++)); do
        svr="${shard_1[idx]}"
        if [ $idx -eq $(( ${#shard_1[@]} - 1 )) ]; then
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_primary 1) > $log_dir/shardsvr_${svr}_1.log 2>&1 &\""
            # sleep 10
        else
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_backup $(get_ip $svr) 1) > $log_dir/shardsvr_${svr}_1.log 2>&1 &\""
        fi
    done

    for ((idx=0; idx<${#shard_2[@]}; idx++)); do
        svr="${shard_2[idx]}"
        if [ $idx -eq $(( ${#shard_2[@]} - 1 )) ]; then
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_primary 2) > $log_dir/shardsvr_${svr}_0.log 2>&1 &\""
            # sleep 10
        else
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_backup $(get_ip $svr) 2) > $log_dir/shardsvr_${svr}_0.log 2>&1 &\""
        fi
    done

    for ((idx=0; idx<${#shard_3[@]}; idx++)); do
        svr="${shard_3[idx]}"
        if [ $idx -eq $(( ${#shard_3[@]} - 1 )) ]; then
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_primary 3) > $log_dir/shardsvr_${svr}_1.log 2>&1 &\""
            # sleep 10
        else
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_backup $(get_ip $svr) 3) > $log_dir/shardsvr_${svr}_1.log 2>&1 &\""
        fi
    done

    for ((idx=0; idx<${#shard_4[@]}; idx++)); do
        svr="${shard_4[idx]}"
        if [ $idx -eq $(( ${#shard_4[@]} - 1 )) ]; then
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_primary 4) > $log_dir/shardsvr_${svr}_0.log 2>&1 &\""
            # sleep 10
        else
            ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sh -c \"cd $ll_dir && nohup $(shard_cmd_backup $(get_ip $svr) 4) > $log_dir/shardsvr_${svr}_0.log 2>&1 &\""
        fi
    done
}

# args: batch_size, rounds
load_keys() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@$client_node "sh -c \"cd $ll_dir && nohup $(basic_be_cmd $1 $2) > $log_dir/basic_be_$client_node.log 2>&1\"" &
    wait
}

# args: num request, time to run, num threads
run_read_bench() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@$client_node "sh -c \"cd $ll_dir && nohup $(read_cmd $1 $2 $3) > $log_dir/read_bench_$client_node.log 2>&1\"" &
    wait
}

kill_shard_svrs() {
    for svr in "${shard_0[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr; sudo pkill -9 shardsvr"
    done 
    for svr in "${shard_1[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr; sudo pkill -9 shardsvr"
    done 
    for svr in "${shard_2[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr; sudo pkill -9 shardsvr"
    done 
    for svr in "${shard_3[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr; sudo pkill -9 shardsvr"
    done 
    for svr in "${shard_4[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr; sudo pkill -9 shardsvr"
    done 
}

kill_dur_svrs() {
    for svr in "${dur_svrs[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 dursvr; sudo pkill -9 dursvr"
    done 
}

kill_cons_svr() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@$cons_svr "sudo pkill -2 conssvr; sudo pkill -9 conssvr"
}

kill_clients() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@$client_node "sudo pkill -9 basic_be" 
    ssh -o StrictHostKeyChecking=no -i $pe $username@$client_node "sudo pkill -9 read_bench"
}

drop_shard_caches() {
    for svr in "${shard_0[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""
    done 
}

collect_logs() {
    for svr in "${shard_0[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:$log_dir/*" "$ll_dir/logs/"
    done 
    for svr in "${shard_1[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:$log_dir/*" "$ll_dir/logs/"
    done 
    for svr in "${shard_2[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:$log_dir/*" "$ll_dir/logs/"
    done 
    for svr in "${shard_3[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:$log_dir/*" "$ll_dir/logs/"
    done 
    for svr in "${shard_4[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:$log_dir/*" "$ll_dir/logs/"
    done 
    scp -o StrictHostKeyChecking=no -i $pe -r "$username@$cons_svr:$log_dir/*" "$ll_dir/logs/"
    for svr in "${dur_svrs[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:$log_dir/*" "$ll_dir/logs/"
    done 
    scp -o StrictHostKeyChecking=no -i $pe -r "$username@$client_node:$log_dir/*" "$ll_dir/logs/"
    scp -o StrictHostKeyChecking=no -i $pe -r "$username@$client_node_1:$log_dir/*" "$ll_dir/logs/"

}

clear_nodes() {
    for svr in "${shard_0[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo rm -rf $data_dir/*"
    done 
    for svr in "${shard_1[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo rm -rf $data_dir/*"
    done 
    for svr in "${shard_2[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo rm -rf $data_dir/*"
    done 
    for svr in "${shard_3[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo rm -rf $data_dir/*"
    done 
    for svr in "${shard_4[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo rm -rf $data_dir/*"
    done 
    ssh -o StrictHostKeyChecking=no -i $pe $username@$cons_svr "sudo rm -rf $data_dir/*"
    for svr in "${dur_svrs[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo rm -rf $data_dir/*"
    done
}

setup_data() {
    clear_nodes
    for svr in "${shard_0[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "mkdir -p $log_dir"
    done 
    for svr in "${shard_1[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "mkdir -p $log_dir"
    done 
    for svr in "${shard_2[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "mkdir -p $log_dir"
    done 
    for svr in "${shard_3[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "mkdir -p $log_dir"
    done 
    for svr in "${shard_4[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "mkdir -p $log_dir"
    done 
    ssh -o StrictHostKeyChecking=no -i $pe $username@$cons_svr "mkdir -p $log_dir"
    for svr in "${dur_svrs[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "mkdir -p $log_dir"
    done 
}

# mode 
#   0 -> run expt
#   

mode="$1"
clients=("10" "16")
if [ "$mode" -eq 0 ]; then # run expt   
    for clients in "${clients[@]}";
    do 
        echo "Running for $clients clients"
        kill_shard_svrs
        kill_cons_svr
        kill_dur_svrs
        kill_clients

        run_shard_svr
        run_dur_svrs 18
        run_cons_svr

        drop_shard_caches
        run_read_bench 10000000 180 $clients
        kill_shard_svrs
        kill_dur_svrs
        kill_cons_svr
        collect_logs
        sudo mkdir ${ll_dir}/logs_$clients
        sudo mv $ll_dir/logs/* ${ll_dir}/logs_$clients
    done 
elif [ "$mode" -eq 1 ]; then # load 10 million keys
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs
    kill_clients

    setup_data
    run_shard_svr
    run_dur_svrs 18
    run_cons_svr

    # load 10 million keys
    load_keys 10000 10
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs

    collect_logs
elif [ "$mode" -eq 2 ]; then # run all the shards
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs

    setup_data
    run_shard_svr
    run_dur_svr_pri 18
    # run_cons_svr
elif [ "$mode" -eq 3 ]; then # run a single shard
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs

    setup_data
    run_single_shard_svr
    run_dur_svr_pri 18
    # run_cons_svr
else
    kill_shard_svrs
    kill_cons_svr
    kill_dur_svrs
    kill_clients
    collect_logs
fi