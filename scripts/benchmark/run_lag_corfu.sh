#!/bin/bash

set -x

username="luoxh"
usergroup="rasl-PG0"
ssh_key="/users/$username/.ssh/id_rsa_ae"
ll_dir="/proj/rasl-PG0/LL-AE/LazyLog-Artifact"
benchmark_dir="${ll_dir}/scripts/benchmark"
cfg_dir="${benchmark_dir}/cfg"
local_log_dir="${benchmark_dir}/logs"
data_dir="/data"
log_dir="$data_dir/logs"
workload="${benchmark_dir}/workloads/erwin-lag.yaml"
be_config="${cfg_dir}/be.prop"
producer_nodes=("node3")
consumer_nodes=("node3")
cons_svr="node0"
dur_svrs=("node1")
shard_0=("node6" "node7" "node8")
rates=("15000" "30000" "45000")
consumer_delay=("0" "3")

get_ip() {
    ip=$(ssh -o StrictHostKeyChecking=no -i $ssh_key $username@$1 "ifconfig | grep 'netmask 255.255.255.0'")
    ip=$(echo $ip | awk '{print $2}')
    echo $ip
}

run_producer_consumer() {
    node="${consumer_nodes[0]}"
    ssh -i $ssh_key $username@$node "cd ${ll_dir}/build/src/benchmark && sudo ./benchmark -c b -f $1 -t lazylog \
        -P ${cfg_dir}/dl_client.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -p dur_log.client_uri=$(get_ip $node):31851\
        -p shard.client_uri=$(get_ip $node):31861 -l ${local_log_dir}/$2/pc_lat.log -T ${local_log_dir}/$2/pc_Tlat.log\
        -L ${local_log_dir}/$2/pc_tail.log" > ${local_log_dir}/$2/pc.log 2>&1 &
}

run_producer_sync() {
    node="${consumer_nodes[0]}"
    ssh -i $ssh_key $username@$node "cd ${ll_dir}/build/src/benchmark && sudo ./benchmark -c b -f $1 -t lazylog \
        -P ${cfg_dir}/dl_client.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -m s -p dur_log.client_uri=$(get_ip $node):31851\
        -p shard.client_uri=$(get_ip $node):31861 -o ${local_log_dir}/$2/pc_produce.log \
        -l ${local_log_dir}/$2/pc_consume.log" > ${local_log_dir}/$2/pc.log 2>&1 &
}

run_producer_consumer_lag() {
    node="${consumer_nodes[0]}"
    ssh -i $ssh_key $username@$node "cd ${ll_dir}/build/src/benchmark && sudo ./benchmark -c b -f $1 -t lazylog \
        -P ${cfg_dir}/dl_client.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -p dur_log.client_uri=$(get_ip $node):31851\
        -p shard.client_uri=$(get_ip $node):31861 -l ${local_log_dir}/$2/pc_consume.log \
        -o ${local_log_dir}/$2/pc_produce.log" > ${local_log_dir}/$2/pc.log 2>&1 &
}

kill_shard_svrs() {
    for svr in "${shard_0[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $ssh_key $username@$svr "sudo pkill -2 shardsvr; sudo pkill -9 shardsvr"
    done 
}

kill_dur_svrs() {
    for svr in "${dur_svrs[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $ssh_key $username@$svr "sudo pkill -2 dursvr; sudo pkill -9 dursvr"
    done 
}

kill_cons_svr() {
    ssh -o StrictHostKeyChecking=no -i $ssh_key $username@$cons_svr "sudo pkill -2 conssvr; sudo pkill -9 conssvr"
}

kill_clients() {
    for cli in "${producer_nodes[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $ssh_key $username@$cli "sudo pkill -9 benchmark"
    done
    for cli in "${consumer_nodes[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $ssh_key $username@$cli "sudo pkill -9 benchmark"
    done
}

collect_logs() {
    for svr in "${shard_0[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $ssh_key -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $ssh_key $username@$svr "rm ${log_dir}/*"
    done 
    scp -o StrictHostKeyChecking=no -i $ssh_key -r "$username@${cons_svr}:${log_dir}/*" "${local_log_dir}/$1/"
    ssh -i $ssh_key $username@${cons_svr} "rm ${log_dir}/*"
    for svr in "${dur_svrs[@]}";
    do
        scp -o StrictHostKeyChecking=no -i $ssh_key -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $ssh_key $username@$svr "rm ${log_dir}/*"
    done 
}

# mkdir -p ${local_log_dir}
# sudo rm -rf ${local_log_dir}/*

# setting single shard
sed -i "s/shard.num=.*/shard.num=1/" $be_config
sed -i "s/shard.threadcount=.*/shard.threadcount=9/" $be_config
echo "set to single shard"

mkdir -p ${local_log_dir}/corfu-lag
sudo rm -rf ${local_log_dir}/corfu-lag/*

for ((j=0; j<${#rates[@]}; j++))
do
    for ((k=0; k<${#consumer_delay[@]}; k++))
    do
        sleep=${rates[$j]}
        delay=${consumer_delay[$k]}
        echo "running with rate $sleep and delay $delay"


        mkdir -p ${local_log_dir}/corfu-lag/${sleep}_${delay}
        sudo rm -rf ${local_log_dir}/corfu-lag/${sleep}_${delay}/*

        kill_clients
        sudo ./run.sh 3

        sed -i "s/producerRate: .*/producerRate: $sleep/" $workload
        sed -i "s/consumerRate: .*/consumerRate: $sleep/" $workload
        sed -i "s/consumerDelayMilli: .*/consumerDelayMilli: $delay/" $workload
        echo "set producer rate to $sleep and consumer rate to $sleep"

        run_producer_consumer_lag $workload corfu-lag/${sleep}_${delay}
        wait

        kill_shard_svrs
        kill_cons_svr
        kill_dur_svrs
        kill_clients
        collect_logs "corfu-lag/${sleep}_${delay}"
    done
done