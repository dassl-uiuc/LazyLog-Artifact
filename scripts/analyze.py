import os
import glob

base_dir = "../"

print("#clients, avg tput(ops/sec), avg latency(us/op), p99(us/op), p99.9(us/op), dir")
for dir_name in os.listdir(base_dir):
    if dir_name.startswith("logs") and os.path.isdir(os.path.join(base_dir, dir_name)):
        clients=None
        avg_tput=0
        avg_latency=0
        p50=0
        p95=0
        p99=0
        p999=0
        num_files=0
        for log_file in glob.glob(os.path.join(base_dir, dir_name, "append_bench_*.log")):
            clients=dir_name.split('_')[1]
            num_files+=1
            with open(log_file, 'r') as file:
                for line in file:
                    if "ops/sec" in line:
                        avg_tput += float(line.split()[-2])
                    if "#[Mean" in line:
                        avg_latency += float(line.split()[2].split(',')[0]) / 1000
                    if "p50:" in line:
                        p50 += float(line.split()[1]) / 1000
                    if "p95:" in line:
                        p95 += float(line.split()[1]) / 1000
                    if "p99:" in line:
                        p99 += float(line.split()[1]) / 1000
                    if "p99.9" in line:
                        p999 += float(line.split()[1]) / 1000
        print(f"{clients:<8}, {avg_tput:<17.3f}, {avg_latency/num_files:<18.3f}, {p99/num_files:<10.3f}, {p999/num_files:<12.3f}, {dir_name}")