get_ip() {
    ip=$(ssh -o StrictHostKeyChecking=no -i $pe $username@$1 "ifconfig | grep 'netmask 255.255.255.0'")
    ip=$(echo $ip | awk '{print $2}')
    echo $ip
}

change_payload_file() {
    sed -i "s|payloadFile: .*|payloadFile: \"$ll_dir/scripts/benchmark/payload/payload-4Kb.data\"|" $workload
}
