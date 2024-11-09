#!/bin/bash

set -x

mode=$1  # start stop
num=$2  # number of shards

primary=(node-5 node-7 node-9 node-11 node-13)
backup=(node-6 node-8 node-10 node-12 node-14)
username=luoxh
usergroup=rasl-PG0

pe="/users/$username/.ssh/id_rsa"
data_dir=/data
log_dir="$data_dir/logs"
ll_dir=/proj/rasl-PG0/$username/LazyLog

# arg: node to ssh into
get_ip() {
    ip=$(ssh -o StrictHostKeyChecking=no -i $pe $username@$1 "ifconfig | grep 'netmask 255.255.255.0'")
    ip=$(echo $ip | awk '{print $2}')
    echo $ip
}

# arg: shard id
shard_cmd_primary() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/storage/shardsvr -P cfg/be.prop -P cfg/shard$1.prop -P cfg/rdma.prop -p leader=true -p shard.num=$num"
}

# arg: shard id, ip addr
shard_cmd_backup() {
    echo "sudo GLOG_minloglevel=1 ./build/src/cons_log/storage/shardsvr -P cfg/be.prop -P cfg/shard$1.prop -P cfg/rdma.prop -p shard.server_uri=$2:31860 -p shard.num=$num"
}

#arg: port num
kill_cmd() {
    echo "pid=\$(sudo lsof -i :$1 | awk 'NR==2 {print \$2}') ; sudo kill -9 \$pid 2> /dev/null || true" 
}

# arg: shard id
start_shard() {
    id=$1
    ssh -o StrictHostKeyChecking=no -i $pe $username@${backup[$id]}     "sh -c \"cd $ll_dir && sudo nohup $(shard_cmd_backup $id $(get_ip ${backup[$id]})) > $log_dir/shardsvr_$id.log 2>&1 &\""
    ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[$id]}    "sh -c \"cd $ll_dir && sudo nohup $(shard_cmd_primary $id) > $log_dir/shardsvr_$id.log 2>&1 &\""
}

# arg: shard id
stop_shard() {
    id=$1
    ssh -o StrictHostKeyChecking=no -i $pe $username@${backup[$id]} "$(kill_cmd 31860)"
    ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[$id]} "$(kill_cmd 31860)"
}

setup_data() {
    for (( i=0; i<$num; i++ ))
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@${backup[$i]} "sudo mkdir -p $log_dir; sudo chown -R $username:$usergroup $log_dir"
        ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[$i]} "sudo mkdir -p $log_dir; sudo chown -R $username:$usergroup $log_dir"
    done
}

clean_data() {
    for (( i=0; i<$num; i++ ))
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@${backup[$i]} "sudo rm -rf $data_dir/*"
        ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[$i]} "sudo rm -rf $data_dir/*"
    done
}

clean

if [ $mode == "start" ]; then
    setup_data
    for (( i=0; i<$num; i++ ))
    do
        start_shard $i
    done
elif [ $mode == "stop" ]; then
    for (( i=0; i<$num; i++ ))
    do
        stop_shard $i
    done
    clean_data
else
    for (( i=0; i<5; i++ ))
    do
        ssh -o StrictHostKeyChecking=no -i $pe $username@${backup[$i]} "sudo mount node-0:/users/$username/LazyLog /users/$username/LazyLog"
        ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[$i]} "sudo mount node-0:/users/$username/LazyLog /users/$username/LazyLog"
    done
fi
