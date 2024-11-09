# LazyLog: A New Shared Log Abstraction for Low-Latency Applications 
This repo is the artifact for: <to be added>

## Setup
For ease of setup, we request that the source code for LazyLog and eRPC and their binaries be hosted on a network-file system so that all the nodes can share these files. 
Some of the installation steps need to be done on all the nodes (16 is sufficient for Erwin-blackbox) and some such as the compilation need only be done on one node. The scripts expect that the data directory on all the nodes to store the run-time logs as well as the storage for the shared-log be mounted at `/data` on each node. **For the benefit of the reviewers, we will provide a cluster which already has all the following setup steps completed**. 

### Installation to be done on all the nodes
* Install RDMA drivers. This step needs to be done on all the nodes in the cluster and the nodes must be rebooted after this step completes
```
cd scripts
./install_mlnx.sh
```
* Install dependencies
```
cd scripts
./deps.sh
```
* Configure huge-pages
```
echo 2048 | sudo tee /proc/sys/vm/nr_hugepages
```
* Own the data directory
```
sudo chown -R <username> /data
```

### Installation to be done on any one node
The following steps assume that the network file-system is at `/sharedfs`
* Get and install eRPC 
```
cd /sharedfs
git clone https://github.com/erpc-io/eRPC
git checkout 793b2a93591d372519983fe23ea4e438199f2462
cmake . -DPERF=ON -DTRANSPORT=infiniband -DROCE=ON -DLOG_LEVEL=info
make -j
```
* Get and install LazyLog
```
cd /sharedfs
git clone https://github.com/dassl-uiuc/LazyLog-Artifact.git --recursive
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```
## Running a simple append-only benchmark on the provided cluster

### Node list
| node   | address                |
|--------|------------------------|
| node0  | hp133.utah.cloudlab.us |
| node1  | hp151.utah.cloudlab.us |
| node2  | hp143.utah.cloudlab.us |
| node3  | hp140.utah.cloudlab.us |
| node4  | hp136.utah.cloudlab.us |
| node5  | hp123.utah.cloudlab.us |
| node6  | hp125.utah.cloudlab.us |
| node7  | hp142.utah.cloudlab.us |
| node8  | hp134.utah.cloudlab.us |
| node9  | hp157.utah.cloudlab.us |
| node10 | hp138.utah.cloudlab.us |
| node11 | hp154.utah.cloudlab.us |
| node12 | hp132.utah.cloudlab.us |
| node13 | hp155.utah.cloudlab.us |
| node14 | hp127.utah.cloudlab.us |
| node15 | hp158.utah.cloudlab.us |

### Steps for Functional
* Login into `node0` in the cluster we provide (unless otherwise stated, all experiment scripts are run from node0). The eRPC and LazyLog directories will be in the shared NFS folder at `/proj/rasl-PG0/LL-AE`. 
  ```
  ssh -o StrictHostKeyChecking=no -i ${PATH_TO_KEY} luoxh@${HOST_ADDR_NODE0}
  ```
* Modify your username and passless private key path in `scripts/run.sh`. 
* Run the following 
```
# Create a logs directory
mkdir -p logs

# Run the script
cd scripts
./run.sh 2
```
* The script setups up the various Erwin-blackbox components (such as the shard servers and sequencing layer servers) and starts an append-only experiment on Erwin-blackbox with 1 backend shard, 1 client thread on `node0` and 4K sized messages. The benchmark should run for approximately 2 minutes and terminate. On termination, in the root directory of LazyLog, a folder with the name `logs_<num_client>_<message_size>_<num_shards>` is created which contains the runtime log file with the latency and throughput metrics. 
* We provide an analysis script to display the standard metrics in a human readable form which can be invoked as 
```
cd scripts
python3 analyze.py
```
* If you wish the change the number of clients and message size, they can be modified in [these lines](https://github.com/dassl-uiuc/LazyLog-Artifact/blob/465c9614e221845f77f3d2f425f47a48f21090b3/scripts/run.sh#L276-L279) in the `run.sh` script. 

### Steps for Results

#### Append Latency (fig 6&7)
To get latency for Erwin, run:
```bash
# rebuild to work with Erwin
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=OFF
cmake --build build -j

cd scripts
sudo rm -rf ../logs*
./fig6.sh
./fig7.sh
python3 analyze.py
```
To get latency for Scalog, run:
```bash
cd scalog-benchmarking/lazylog-benchmarking/scripts
sudo rm -rf ../results
./run_script_on_all.sh setup_disk.sh
./run.sh 0
python3 analyze.py
```
To get the latency for Corfu, run:
```bash
# rebuild to work with Corfu
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=ON
cmake --build build -j

cd scripts/benchmark
sudo rm -rf ./logs
./run_single_shard.sh
python3 avg_lat.py ./logs/corfu-single-shard/pc_produce.log  # get the latency for single shard

./run_five_shard.sh
python avg_lat.py logs/corfu-five-shard/pc_produce_node3.log logs/corfu-five-shard/pc_produce_node4.log  # get the latency for five shard
```

#### Read Latency (fig 8&9)
Run the scripts for corfu and erwin. Each step runs for 6 different parameters (rate: 15K, 30K, 45K)x(delay: 0ms, 3ms) with 2 minutes per config. Consequently, the total runtime for corfu and erwin is about 24 mins
```bash
# rebuild to work with Corfu
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=ON
cmake --build build -j

cd scripts/benchmark
sudo rm -rf ./logs

# run with corfu (6 mins)
./run_lag_corfu.sh

# rebuild to work with Erwin
cd ../..
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=OFF
cmake --build build -j

# run with erwin (6 mins)
cd scripts/benchmark
./run_lag_erwin.sh

# analyze and print all results
python3 analyze_fig_8_9.py
```  

#### Append Throughput (fig 12)
To get the throughput for Erwin, run:
```bash
cd scripts
sudo rm -rf ../logs*
./fig12.sh
python3 analyze.py
```

## Supported Platforms
The two lazylog systems Erwin-blackbox and Erwin-st have been tested on the following platforms
* OS: Ubuntu 22.04 LTS
* NIC: Mellanox MT27710 Family ConnectX-4 Lx (25Gb RoCE)
* RDMA Driver: MLNX_OFED-23.10-0.5.5.0









