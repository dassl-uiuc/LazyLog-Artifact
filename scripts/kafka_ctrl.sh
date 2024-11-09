#!/bin/bash

set -x

mode=$1
num=$2

primary=(node-5 node-7 node-9 node-11 node-13)
backup=(node-6 node-8 node-10 node-12 node-14)
username=luoxh
usergroup=rasl-PG0

pe="/users/$username/.ssh/id_rsa"
data_dir=/data
log_dir="$data_dir/logs"
kafka_dir=/proj/rasl-PG0/$username/kafka
zk_dir=$data_dir/zookeeper
topic=default-topic

# arg: node to ssh into
get_ip() {
    ip=$(ssh -o StrictHostKeyChecking=no -i $pe $username@$1 "ifconfig | grep 'netmask 255.255.255.0'")
    ip=$(echo $ip | awk '{print $2}')
    echo $ip
}

start_zk() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[0]}  "sh -c \"cd $kafka_dir && ./bin/zookeeper-server-start.sh ./config/zookeeper.properties > $log_dir/zk.log 2>&1 & \" "
}

stop_zk() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[0]} "sh -c \"cd $kafka_dir && ./bin/zookeeper-server-stop.sh \" "
}

start_shard() {
    id=$1
    ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[$id]}    "sh -c \"cd $kafka_dir && eatmydata ./bin/kafka-server-start.sh ./config/server.properties --override broker.id=$((id * 2)) --override log.dirs=$data_dir/kafka-logs > $log_dir/broker_$((id*2)).log 2>&1 & \""
    ssh -o StrictHostKeyChecking=no -i $pe $username@${backup[$id]}     "sh -c \"cd $kafka_dir && eatmydata ./bin/kafka-server-start.sh ./config/server.properties --override broker.id=$((id * 2 + 1)) --override log.dirs=$data_dir/kafka-logs > $log_dir/broker_$((id*2+1)).log 2>&1 & \""
}

stop_shard() {
    id=$1
    ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[$id]}    "sh -c \"cd $kafka_dir && ./bin/kafka-server-stop.sh \""
    ssh -o StrictHostKeyChecking=no -i $pe $username@${backup[$id]}     "sh -c \"cd $kafka_dir && ./bin/kafka-server-stop.sh \""
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

setup_kafka() {
    ssh -o StrictHostKeyChecking=no -i $pe $username@${primary[0]} "sh -c \" cd $kafka_dir && ./bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic $topic --create --partitions $num --replication-factor 2 \""
}

if [ $mode == "start" ]; then
    setup_data
    start_zk
    sleep 2
    for (( i=0; i<$num; i++ ))
    do
        start_shard $i
    done
    sleep 2
    setup_kafka
elif [ $mode == "stop" ]; then
    for (( i=0; i<$num; i++ ))
    do
        stop_shard $i
    done
    stop_zk
    sleep 2
    clean_data
else
    echo "start or stop"
fi
