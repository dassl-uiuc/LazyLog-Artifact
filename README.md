# LazyLog: A New Shared Log Abstraction for Low-Latency Applications 
This repo is the artifact for:

[**LazyLog: A New Shared Log Abstraction for Low-Latency Applications**](https://doi.org/10.1145/3694715.3695983)

Xuhao Luo, Shreesha Bhat*, Jiyu Hu*, Ramnatthan Alagappan, Aishwarya Ganesan

(*=joint second authorship)

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
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=OFF
cmake --build build -j
```
## Running a simple append-only benchmark on the provided cluster

### Node list
| node   | address                |
|--------|------------------------|
| node0  | hp027.utah.cloudlab.us |
| node1  | hp035.utah.cloudlab.us |
| node2  | hp086.utah.cloudlab.us |
| node3  | hp087.utah.cloudlab.us |
| node4  | hp111.utah.cloudlab.us |
| node5  | hp117.utah.cloudlab.us |
| node6  | hp120.utah.cloudlab.us |
| node7  | hp028.utah.cloudlab.us |
| node8  | hp088.utah.cloudlab.us |
| node9  | hp106.utah.cloudlab.us |
| node10 | hp108.utah.cloudlab.us |
| node11 | hp109.utah.cloudlab.us |
| node12 | hp107.utah.cloudlab.us |
| node13 | hp081.utah.cloudlab.us |
| node14 | hp119.utah.cloudlab.us |
| node15 | hp012.utah.cloudlab.us |

### Steps for Functional
* Login into `node0` in the cluster we provide (unless otherwise stated, all experiment scripts are run from node0). The eRPC and LazyLog directories will be in the shared NFS folder at `/proj/rasl-PG0/LL-AE`. 
  ```
  ssh -o StrictHostKeyChecking=no -i ${PATH_TO_KEY} luoxh@${HOST_ADDR_NODE0}
  ```
* Modify your username and passless private key path in `scripts/usr_cfg.sh`. (already done)
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

cp ./cfg/rdma.prop ./cfg_3_way/

cd scripts
sudo rm -rf ../logs*
./fig6.sh
./fig7.sh
python3 analyze.py
```
To get latency for Scalog, first build scalog:
```bash
# install go on all nodes (if you haven't done so)
cd scalog-benchmarking/lazylog-benchmarking/scripts
./run_script_on_all.sh install_go.sh
source ~/.bashrc

# build scalog
sudo chown -R $(whoami) ~/go
sudo rm -rf ~/.cache
cd ../..  # now in `scalog-benchmark` dir
go build
# You should see `scalog` executable in scalog-benchmarking dir
```

Then run:
```bash
cd lazylog-benchmarking/scripts
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

# copy the rdma config
cp ./cfg/rdma.prop ./scripts/benchmark/cfg/

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

#### Scalable Throughput (fig 13)
To get the throughput for Erwin on 1, 3, and 5 shards, run:
```bash
# build the regular version of Erwin
git checkout main
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=OFF
cmake --build build -j

cd scripts
sudo rm -rf ../logs*
./fig13.sh
python3 analyze.py
```
To get the latency vs. throughput for Erwin, run:
```bash
cd scripts
sudo rm -rf ../logs*  # analyze the previous results before removing them
./fig13b.sh
python3 analyze.py
```

To get the throughput for Erwin-st on 1, 3, and 5 shards, run
```bash
# build the scalable-tput (st) version of Erwin
git checkout scalable-tput
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCORFU=OFF
cmake --build build -j

cd scripts
sudo rm -rf ../logs*  # analyze the previous results before removing them
./fig13_st.sh
python3 analyze.py
```
To get the latency vs. throughput for Erwin-st, run:
```bash
cd scripts
sudo rm -rf ../logs*  # analyze the previous results before removing them
./fig13b_st.sh
python3 analyze.py
```

PS: We only run 4KB append here, because running 8KB append requires 4 client nodes for the throughput to be scalable otherwise it will be bottlenecked by the client. We only have 16 machines and can use at most 2 machines for client nodes (in the case of 5 shards).

### Organization and Implementation Notes

At a high-level, the source directory for this artifact is organized as follows
#### Erwin-bb (main branch)
```
LazyLog-Artifact
|- RDMA/ (a C++ RDMA library)
|- cfg*/ (configuration files)
|- src/ (source code for Erwin-bb)
|   |- client/ (client code)
|   |   |- benchmarking/ (benchmarking clients e.g. append client)
|   |   |- lazylog_cli.h (client header)
|   |   |- lazylog_cli.cc (client implementation)
|   |   |- basic_client.cc (simple client to illustrate the basic API)
|   |- dur_log/ (sequencing layer implementation)
|   |   |- dur_svr.cc (sequencing layer server)
|   |   ...
|   |- cons_log/ 
|   |   |- storage/ (shard server implementation)
|   |   |   |- shard_svr.cc (shard server)
|   |   |   ...
|   |   |- cons_svr.cc (component that bridges sequencing layer interaction with the shard server)
|   |   ...
|   |- rpc/ (wrappers around eRPC)
|   |- utils/ (utilities)
|   |- benchmark/ (wrappers around LazyLog-API for append-read mixed experiments)
...
```
#### Erwin-st (scalable-tput branch)
```
LazyLog-Artifact
|- RDMA/ (a C++ RDMA library)
|- cfg*/ (configuration files)
|- src/ (source code for Erwin-st)
|   |- client/ (client code)
|   |   |- benchmarking/ (benchmarking clients e.g. append client)
|   |   |- lazylog_scalable_cli.h (client header)
|   |   |- lazylog_scalable_cli.cc (client implementation)
|   |   |- basic_client.cc (simple client to illustrate the basic API)
|   |   ...
|   |- dur_log/ (sequencing layer implementation)
|   |   |- dur_svr.cc (sequencing layer server, handles metadata only)
|   |   ...
|   |- cons_log/ 
|   |   |- storage/ (shard server implementation)
|   |   |   |- datalog/ 
|   |   |   |   |- datalog_svr.cc (shard data server, handles data writes)
|   |   |   |   ...
|   |   |   ...
|   |   |- cons_svr.cc (component that bridges sequencing layer interaction with the shard data server)
|   |   ...
|   |- rpc/ (wrappers around eRPC)
|   |- utils/ (utilities)
...
```
The sequencing layer server is in the `src/dur_log` subdirectory with the main entry point at `src/dur_log/dur_svr.cc`. The `src/cons_log` subdirectory implements the shard server and bridge between the sequencing layer and the shard servers. The `src/cons_log/storage` subdirectory (`src/cons_log/storage/datalog` for erwin-st) contains the implementation of the shard servers with the main entry point at `src/cons_log/storage/shard_svr.cc` (`src/cons_log/storage/datalog/datalog_svr.cc` for erwin-st). 

The sequencing layer is implemented as a ring-buffer in memory. `src/cons_log/cons_svr.cc` implements the logical portion on the sequencing layer primary that periodically orders records/metadata from the sequencing layer and pushes records/metadata to the shard servers. In our experiments, we run this component on a separate machine from the actual sequencing layer primary and perform RDMA reads and writes to interact with the sequencing layer primary nodes' ring-buffer (to periodically read and unordered records/metadata and flush them to the shard servers; and to garbage collect records and update the `last-ordered-gp` (from the paper)). The `cons_svr` uses RPCs to garbage collect records and update `last-ordered-gp` on the backups. 

Here we list some of the important pieces of the erwin/erwin-st architecture as described in the paper and link it to the actual implementation. 
1) In our code, the `last-ordered-gp` on each sequencing layer node is represented by the [`ordered_watermk_`](https://github.com/dassl-uiuc/LazyLog-Artifact/blob/ab1fffb792700e891d3c66e293d44706383def79/src/dur_log/dur_log_flat.cc#L51) variable. 
2) After ordering and writing a batch of records on the shard servers, the records are garbage collected and `last-ordered-gp` is updated on all the sequencing replicas [here](https://github.com/dassl-uiuc/LazyLog-Artifact/blob/ab1fffb792700e891d3c66e293d44706383def79/src/cons_log/cons_log.cc#L150-L151). 
3) `stable-gp` is updated on the shard servers [here](https://github.com/dassl-uiuc/LazyLog-Artifact/blob/ab1fffb792700e891d3c66e293d44706383def79/src/cons_log/cons_log.cc#L162)


### How to write your own Erwin-st/Erwin-bb client application? 
One can follow the client API in `src/client/lazylog_cli.h` (in `src/client/lazylog_scalable_cli.h` for erwin-st) for all the available RPC calls. An example client is provided in `src/client/basic_client.cc`, for a more advanced example, refer to `src/client/benchmarking/append_bench.cc`. To link erwin-bb/erwin-st with your own application and use the client API,
1) Build the source code (see above). 
2) Include the client header file. 
3) Link lazylogcli (created at `build/src/liblazylogcli.a`) and backendcli (created at `build/src/cons_log/storage/liblazylogcli.a`).


One must ensure that each `LazyLogClient` is created in a separate thread. For best load balancing of client connections across all server eRPC threads, you can set a unique `dur_log.client_id` property for each client before calling `Initialize` as shown [here](https://github.com/dassl-uiuc/LazyLog-Artifact/blob/ab1fffb792700e891d3c66e293d44706383def79/src/client/benchmarking/append_bench.cc#L32-L36). 

## Supported Platforms
The two lazylog systems Erwin-blackbox and Erwin-st have been tested on the following platforms
* OS: Ubuntu 22.04 LTS
* NIC: Mellanox MT27710 Family ConnectX-4 Lx (25Gb RoCE)
* RDMA Driver: MLNX_OFED-23.10-0.5.5.0









