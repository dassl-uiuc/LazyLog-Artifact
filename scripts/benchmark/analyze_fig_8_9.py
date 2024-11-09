import os
import csv

def print_table(directory):
    output_data = [['rate', 'delay (ms)', 'mean append latency (ns)', 'mean read latency (ns)']]
    for dir in os.listdir(directory):
        if not os.path.isdir(os.path.join(directory, dir)):
            continue
        rate = dir.split('_')[0]
        delay = dir.split('_')[1]
        with open(os.path.join(directory, dir, 'pc.log'), 'r') as file:
            lines = file.readlines()
            mean_append_latency, mean_read_latency = lines[-1].strip().split(',')
        output_data.append([rate, delay, mean_append_latency, mean_read_latency])

    # sort rows by rate and delay
    output_data[1:] = sorted(output_data[1:], key=lambda x: (int(x[0]), int(x[1])))

    for row in output_data:
        print(", ".join(map(str, row)))

print("********************************")
print("corfu")
try:
    print_table('./logs/corfu-lag')
except:
    pass
print("********************************")
print("erwin")
try:
    print_table('./logs/erwin-lag')
except:
    pass
print("********************************")



