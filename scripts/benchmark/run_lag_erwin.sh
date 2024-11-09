#!/bin/bash

set -x

username="luoxh"
usergroup="rasl-PG0"
ssh_key="/users/$username/.ssh/id_rsa_ae"
ll_dir="/proj/rasl-PG0/LL-AE/LazyLog-Artifact"
benchmark_dir="${ll_dir}/scripts/benchmark"
cfg_dir="${ll_dir}/cfg_3_way"
local_log_dir="${benchmark_dir}/logs"
data_dir="/data"
log_dir="$data_dir/logs"
workload="${benchmark_dir}/workloads/erwin-lag.yaml"
be_config="${cfg_dir}/be.prop"
producer_nodes=("node0")
consumer_nodes=("node0")
cons_svr="node4"
dur_svrs=("node1" "node2" "node3")
shard_pri=("node5" "node7" "node9" "node11" "node13")
shard_bac=("node6" "node8" "node10" "node12" "node14")
shard_bac1=("node14" "node12" "node8" "node10" "node6")
rates=("15000" "30000" "45000")
consumer_delay=("0" "3")
client_nodes=("node0")

# arg: ip_addr of node, number of threads
dur_cmd() {
    echo "sudo GLOG_minloglevel=1 ./build/src/dur_log/dursvr -P ${cfg_dir}/durlog.prop -P ${cfg_dir}/rdma.prop -p dur_log.server_uri=$1:31850"
}

cons_cmd() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/conssvr -P ${cfg_dir}/conslog.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -P ${cfg_dir}/dl_client.prop"
}

shard_cmd_primary() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/storage/shardsvr -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -P ${cfg_dir}/shard$1.prop -p leader=true"
}

# arg: ip_addr of node
shard_cmd_backup() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/storage/shardsvr -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -P ${cfg_dir}/shard$2.prop -p shard.server_uri=$1:31860"
}

# used when running two shard servers on the same ip. 
# must use 31861 port
# arg: ip_addr of node
shard_cmd_backup_prime() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/storage/shardsvr -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop -P ${cfg_dir}/shard$2.prop -p shard.server_uri=$1:31861"
}

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
        -P ${cfg_dir}/dl_client.prop -P ${cfg_dir}/rdma.prop -P ${cfg_dir}/be.prop \
        -p shard.client_uri=$(get_ip $node):31851 -l ${local_log_dir}/$2/pc_consume.log \
        -o ${local_log_dir}/$2/pc_produce.log" > ${local_log_dir}/$2/pc.log 2>&1 &
}

kill_shard_svrs() {
    for svr in "${shard_pri[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $ssh_key $username@$svr "sudo pkill -2 shardsvr; sudo pkill -9 shardsvr"
    done 
    for svr in "${shard_bac[@]}"; do
        ssh -o StrictHostKeyChecking=no -i $ssh_key $username@$svr "sudo pkill -2 shardsvr; sudo pkill -9 shardsvr"
    done
    for svr in "${shard_bac1[@]}"; do
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
    for svr in "${shard_pri[@]}"; 
    do
        scp -o StrictHostKeyChecking=no -i $ssh_key -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $ssh_key $username@$svr "rm ${log_dir}/*"
    done 
    for svr in "${shard_bac[@]}";
    do
        scp -o StrictHostKeyChecking=no -i $ssh_key -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $ssh_key $username@$svr "rm ${log_dir}/*"
    done
    for svr in "${shard_bac1[@]}";
    do
        scp -o StrictHostKeyChecking=no -i $ssh_key -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $ssh_key $username@$svr "rm ${log_dir}/*"
    done
    scp -o StrictHostKeyChecking=no -i $ssh_key -r "$username@$cons_svr:${log_dir}/*" "${local_log_dir}/$1/"
    ssh -i $ssh_key $username@$cons_svr "rm ${log_dir}/*"
    for svr in "${dur_svrs[@]}";
    do
        scp -o StrictHostKeyChecking=no -i $ssh_key -r "$username@$svr:${log_dir}/*" "${local_log_dir}/$1/"
        ssh -i $ssh_key $username@$svr "rm ${log_dir}/*"
    done 
}


# args: num shards
run_shard_svr() {
    for ((i=0; i<$1; i++)); 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@${shard_bac[$i]} "sh -c \"cd $ll_dir && nohup $(shard_cmd_backup $(get_ip ${shard_bac[$i]}) $i) > $log_dir/shardsvr_backup_$i_${shard_bac[$i]}.log 2>&1 &\""
    done 
    for ((i=0; i<$1; i++)); 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@${shard_bac1[$i]} "sh -c \"cd $ll_dir && nohup $(shard_cmd_backup_prime $(get_ip ${shard_bac1[$i]}) $i) > $log_dir/shardsvr_backup1_$i_${shard_bac1[$i]}.log 2>&1 &\""
    done 
    sleep 2
    for ((i=0; i<$1; i++)); 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@${shard_pri[$i]} "sh -c \"cd $ll_dir && nohup $(shard_cmd_primary $i) > $log_dir/shardsvr_pri_$i_${shard_pri[$i]}.log 2>&1 &\""
    done 
    sleep 2
}

run_dur_svrs() {
    local primary_done=false
    for svr in "${dur_svrs[@]}"; 
    do 
        if ${primary_done}; then 
            ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sh -c \"cd $ll_dir && nohup $(dur_cmd $(get_ip $svr)) > $log_dir/dursvr_$svr.log 2>&1 &\""
        else 
            ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sh -c \"cd $ll_dir && nohup $(dur_cmd $(get_ip $svr)) -p leader=true > $log_dir/dursvr_$svr.log 2>&1 &\""
            primary_done=true
        fi 
    done 
}

clear_nodes() {
    for svr in "${shard_pri[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sudo rm -rf $data_dir/*" &
    done 
    for svr in "${shard_bac[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sudo rm -rf $data_dir/*" &
    done 
    for svr in "${shard_bac1[@]}";
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sudo rm -rf $data_dir/*" &
    done
    ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$cons_svr "sudo rm -rf $data_dir/*" &
    for svr in "${dur_svrs[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sudo rm -rf $data_dir/*" &
    done 
    for client in "${client_nodes[@]}"; 
    do 
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$client "sudo rm -rf $data_dir/*" &
    done
    wait
}

run_cons_svr() {
    ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$cons_svr "sh -c \"cd $ll_dir && nohup $(cons_cmd) > $log_dir/conssvr_$svr.log 2>&1 &\""
}

setup_data() {
    clear_nodes
    for svr in "${shard_pri[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sudo chown -R $username:$usergroup $data_dir; mkdir $log_dir"
    done 
    for svr in "${shard_bac[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sudo chown -R $username:$usergroup $data_dir; mkdir $log_dir"
    done 
    ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$cons_svr "sudo chown -R $username:$usergroup $data_dir; mkdir $log_dir"
    for svr in "${dur_svrs[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$svr "sudo chown -R $username:$usergroup $data_dir; mkdir $log_dir"
    done 
    for client in "${client_nodes[@]}"; 
    do
        ssh -o StrictHostKeyChecking=no -i ${ssh_key} $username@$client "sudo chown -R $username:$usergroup $data_dir; mkdir $log_dir"
    done 
}

# mkdir -p ${local_log_dir}
# sudo rm -rf ${local_log_dir}/*



# setting single shard
sed -i "s/shard.num=.*/shard.num=1/" $be_config
echo "set to single shard"

mkdir -p ${local_log_dir}/erwin-lag
sudo rm -rf ${local_log_dir}/erwin-lag/*

for ((j=0; j<${#rates[@]}; j++))
do
    for ((k=0; k<${#consumer_delay[@]}; k++))
    do
        sleep=${rates[$j]}
        delay=${consumer_delay[$k]}
        echo "running with rate $sleep and delay $delay"

        kill_shard_svrs
        kill_cons_svr
        kill_dur_svrs
        kill_clients

        setup_data
        run_shard_svr 1
        run_dur_svrs
        run_cons_svr

        mkdir -p ${local_log_dir}/erwin-lag/${sleep}_${delay}
        sudo rm -rf ${local_log_dir}/erwin-lag/${sleep}_${delay}/*

        sed -i "s/producerRate: .*/producerRate: $sleep/" $workload
        sed -i "s/consumerRate: .*/consumerRate: $sleep/" $workload
        sed -i "s/consumerDelayMilli: .*/consumerDelayMilli: $delay/" $workload
        echo "set producer rate to $sleep and consumer rate to $sleep"

        run_producer_consumer_lag $workload erwin-lag/${sleep}_${delay}
        wait

        kill_shard_svrs
        kill_cons_svr
        kill_dur_svrs
        kill_clients
        collect_logs "erwin-lag/${sleep}_${delay}"
    done
done