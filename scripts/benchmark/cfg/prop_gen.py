import yaml
import sys
import os
from string import Template

config_path = os.path.dirname(__file__)

def gen_client_prop(client_ip: str, dl_ips: 'list[str]', configs: dict):
    print("client: ", client_ip)
    with open(os.path.join(config_path, 'dl_client.prop.template'), 'r') as t:
        tmpl = Template(t.read())
    with open(os.path.join(config_path, 'dl_client.prop'), 'w') as f:
        lines = []
        lines.append(tmpl.substitute(
            server_uri=','.join([d+':31850' for d in dl_ips]),
            primary_uri=dl_ips[0]+':31850',
            client_uri=client_ip+':31851',
            msg_size=configs['dur_log']['msg_size']
        ))
        f.writelines(lines)


def gen_durlog_prop(dl_ips: 'list[str]', cl_ip:str, configs: dict):
    print("dur log: ", dl_ips)
    with open(os.path.join(config_path, 'durlog.prop.template'), 'r') as t:
        tmpl = Template(t.read())
    with open(os.path.join(config_path, 'durlog.prop'), 'w') as f:
        lines = []
        lines.append(tmpl.substitute(
            server_uri=dl_ips[0]+':31850',
            cl_server_uri=cl_ip+':31852',
            mr_size=configs['dur_log']['mr_size']
        ))
        f.writelines(lines)


def gen_conslog_prop(cl_ip: str, configs: dict):
    print("cons log: ", cl_ip)
    with open(os.path.join(config_path, 'conslog.prop.template'), 'r') as t:
        tmpl = Template(t.read())
    with open(os.path.join(config_path, 'conslog.prop'), 'w') as f:
        lines = []
        lines.append(tmpl.substitute(
            server_uri=cl_ip + ':31852',
            mr_size=configs['cons_log']['mr_size'],
            fetch_size=configs['cons_log']['max_fetch_size'],
            ppl='true' if configs['cons_log']['pipeline'] else 'false',
        ))
        f.writelines(lines)


def gen_backend_prop(shard_ips: 'list[str]', shard_rep: int, n_shard: int, cl_ip: str, configs: dict):
    with open(os.path.join(config_path, 'be.prop.template'), 'r') as t:
        tmpl = Template(t.read())
    with open(os.path.join(config_path, 'be.prop'), 'w') as f:
        primaries = []
        backups = []
        for i in range(0, n_shard * shard_rep, shard_rep):
            primaries.append(shard_ips[i])
            backups.append(shard_ips[i+1])
        print('shards: ', primaries)
        print('backups: ', backups)
        lines = []
        lines.append(tmpl.substitute(
            client_uri=cl_ip+':31861',
            primary_uri=','.join([p+':31860' for p in primaries]),
            backup_uri=','.join([b+':31860' for b in backups]),
            msg_size=configs['backend']['msg_size'],
            stripe_unit=configs['backend']['stripe_unit'],
            shard_num=n_shard,
            folder=configs['backend']['path']
        ))
        lines.append("shard.threadcount=18")
        f.writelines(lines)


def gen_shard_prop(shard_ips: 'list[str]', shard_rep: int, n_shard: int):
    with open(os.path.join(config_path, 'shard.prop.template'), 'r') as t:
        tmpl = Template(t.read())
    
    id = 0
    for i in range(0, n_shard * shard_rep, shard_rep):
        with open(os.path.join(config_path, 'shard{}.prop'.format(id)), 'w') as f:
            lines = []
            lines.append(tmpl.substitute(
                server_uri=shard_ips[i]+':31860',
                backup_uri=shard_ips[i+1] + ':31860' if shard_rep > 1 else '',
                shard_id=int(i / shard_rep)
            ))
            lines.append("threadcount=18")
            f.writelines(lines)
        id += 1

def gen_prop(n_shard: int):
    with open(os.path.join(config_path, 'config.yaml'), 'r') as f:
        configs = yaml.safe_load(f)
        nodes = configs['nodes']
        client_ip = nodes[0]
        n_dl = configs['dur_log']['rep']
        dur_server_ips = nodes[1:1+n_dl]
        cons_server_ip = nodes[1+n_dl]
        shard_rep = configs['backend']['rep']
        if 1+n_dl+1+n_shard * shard_rep > len(nodes):
            print("Error: no enough nodes")
            return
        shard_ips = nodes[1+n_dl+1:1+n_dl+1+n_shard*shard_rep]

        gen_client_prop(client_ip, dur_server_ips, configs)
        gen_durlog_prop(dur_server_ips, cons_server_ip, configs)
        gen_conslog_prop(cons_server_ip, configs)
        gen_backend_prop(shard_ips, shard_rep, n_shard, cons_server_ip, configs)
        gen_shard_prop(shard_ips, shard_rep, n_shard)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        n_shard = int(sys.argv[1])
    else:
        n_shard = 1
    gen_prop(n_shard)
    print("\nIt's STRONGLY advised to manually check the generated prop files")
