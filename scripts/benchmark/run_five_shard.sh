#!/bin/bash

set -x

source $(dirname $0)/../usr_cfg.sh
source $(dirname $0)/common.sh
ll_dir=$(realpath $(dirname $0)/../..)

benchmark_dir="${ll_dir}/scripts/benchmark"
cfg_dir="${benchmark_dir}/cfg"
local_log_dir="${benchmark_dir}/logs"
log_dir="$data_dir/logs"
workload="${benchmark_dir}/workloads/corfu-five-shard.yaml"
be_config="${cfg_dir}/be.prop"
# producer_nodes=("node0")
consumer_nodes=("node3" "node4")
# consumer_nodes=("node3")
cons_svr="node0"
dur_svrs=("node1")
shard_0=("node6" "node7" "node8")
shard_1=("node6" "node7" "node8")
shard_2=("node9" "node10" "node11")
shard_3=("node9" "node10" "node11")
shard_4=("node12" "node13" "node14")

run_producer_consumer() {
    node="${consumer_nodes[0]}"
    ssh -i $pe $username@$node "cd ${ll_dir}/build/src/benchmark && sudo ./benchmark -c b -f $1 -t lazylog \
        -P ${cfg_dir}/dl_client.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -p dur_log.client_uri=$(get_ip $node):31851\
        -p shard.client_uri=$(get_ip $node):31861 -l ${local_log_dir}/$2/pc_lat.log -T ${local_log_dir}/$2/pc_Tlat.log\
        -L ${local_log_dir}/$2/pc_tail.log" > ${local_log_dir}/$2/pc.log 2>&1 &
}

run_producer_sync() {
    node="${consumer_nodes[0]}"
    ssh -i $pe $username@$node "cd ${ll_dir}/build/src/benchmark && sudo ./benchmark -c b -f $1 -t lazylog \
        -P ${cfg_dir}/dl_client.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -m s \
        -p shard.client_uri=$(get_ip $node):31861 -p dur_log.client_uri=$(get_ip $node):31851 \
        -o ${local_log_dir}/$2/pc_produce_${node}.log -i 0 -l ${local_log_dir}/$2/pc_consume_${node}.log" \
        > ${local_log_dir}/$2/pc_${node}.log 2>&1 &

    node="${consumer_nodes[1]}"
    ssh -i $pe $username@$node "cd ${ll_dir}/build/src/benchmark && sudo ./benchmark -c b -f $1 -t lazylog \
        -P ${cfg_dir}/dl_client.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -m s \
        -p shard.client_uri=$(get_ip $node):31861 -p dur_log.client_uri=$(get_ip $node):31851 \
        -o ${local_log_dir}/$2/pc_produce_${node}.log -i 10 -l ${local_log_dir}/$2/pc_consume_${node}.log" \
        > ${local_log_dir}/$2/pc_${node}.log 2>&1 &
}

kill_shard_svrs() {
    for svr in "${shard_0[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr"
    done 

    for svr in "${shard_1[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr"
    done 

    for svr in "${shard_2[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr"
    done 

    for svr in "${shard_3[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr"
    done 

    for svr in "${shard_4[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 shardsvr"
    done 
}

kill_dur_svrs() {
    for svr in "${dur_svrs[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$svr "sudo pkill -2 dursvr"
    done 
}

kill_cons_svr() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@$cons_svr "sudo pkill -2 conssvr"
}

kill_clients() {
    for cli in "${producer_nodes[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$cli "sudo pkill -9 benchmark"
    done
    for cli in "${consumer_nodes[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $pe $username@$cli "sudo pkill -9 benchmark"
    done
}

collect_logs() {
    for svr in "${shard_0[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $pe $username@$svr "rm ${log_dir}/*"
    done 

    for svr in "${shard_1[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $pe $username@$svr "rm ${log_dir}/*"
    done 

    for svr in "${shard_2[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $pe $username@$svr "rm ${log_dir}/*"
    done 

    for svr in "${shard_3[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $pe $username@$svr "rm ${log_dir}/*"
    done 

    for svr in "${shard_4[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $pe $username@$svr "rm ${log_dir}/*"
    done 

    scp -o StrictHostKeyChecking=no -i $pe -r "$username@${cons_svr}:${log_dir}/*" "${local_log_dir}/$1/"
    ssh -i $pe $username@${cons_svr} "rm ${log_dir}/*"
    for svr in "${dur_svrs[@]}";
    do
        scp -o StrictHostKeyChecking=no -i $pe -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $pe $username@$svr "rm ${log_dir}/*"
    done 
}

# mkdir -p ${local_log_dir}
# sudo rm -rf ${local_log_dir}/*

kill_clients

# setting 5 shards
sed -i "s/shard.num=.*/shard.num=5/" $be_config
sed -i "s/shard.threadcount=.*/shard.threadcount=9/" $be_config
echo "set to five shards"
change_payload_file $workload

sudo ./run.sh 2

mkdir -p ${local_log_dir}/corfu-five-shard
sudo rm -rf ${local_log_dir}/corfu-five-shard/*

# run_spec_read $workload
# run_producer_consumer $workload $sleep
run_producer_sync $workload "corfu-five-shard"
wait

kill_shard_svrs
kill_cons_svr
kill_dur_svrs
kill_clients
collect_logs "corfu-five-shard"

# run the analysis
echo "analyze produce latencies..."
python avg_lat.py logs/corfu-five-shard/pc_produce_node3.log logs/corfu-five-shard/pc_produce_node4.log 

echo "analyze consume latencies..."
python avg_lat.py logs/corfu-five-shard/pc_consume_node3.log logs/corfu-five-shard/pc_consume_node4.log 