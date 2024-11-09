import sys

from RDMA.rdma_config import get_net_config
from RDMA.rdma_config import choose_link


def write_config(link: dict, path: str):
    lines: list = []
    with open(path, 'w') as f:
        lines.append('erpc.phy_port={}\n'.format(link['glbl_idx']))
        lines.append('rdma.dev_name={}\n'.format(link['link']))
        lines.append('eth.dev_name={}\n'.format(link['netdev']))
        lines.append('rdma.ip_port_ctrl={}\n'.format(8011))
        f.writelines(lines)


if __name__ == '__main__':
    cfg_path = sys.argv[1]
    write_config(choose_link(get_net_config()), cfg_path)
