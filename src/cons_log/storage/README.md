# Guide for Configuration

## Backend (consensus log) Side
`cfg/be.prop`
```bash
shard.client_uri={ip1}:31861  # uri of the CL (backend) server itself
shard.primary_uri={ip2}:31860  # uri of primary shard servers (comma separated)
shard.stripe_unit_size=1000  # max number of entries to send to shard in a single RPC
```

## Shard Side
`cfg/shard*.prop`
### Primary
```bash
shard.server_uri={ip2}:31860  # uri of the primary shard server itself
shard.backup_uri={ip3}:31860  # uri of backup servers for its shard (comma separated)
shard.stripe_unit_size=1000  # same as above, also the number of entries in each data file
shard.num=1  # number of shards in the backend
shard.id=0  # id of this shards (must be equal to position of server_uri in primary_uri)
```
### Backup
```bash
shard.server_uri={ip3}:31860  # uri of the backup shard server itself
shard.stripe_unit_size=1000  # same as primary
shard.num=1  # number of shards in the backend
shard.id=0  # must be equal to the primary's id
```

## Example
If we have 2 shards, each shard has one primary and one backup
- shard 0:
    - pri: 10.10.1.1:31860
    - back: 10.10.1.2:31860
- shard 1:
    - pri: 10.10.1.3:31860
    - back: 10.10.1.4:31860

```bash
# shard0.prop
shard.server_uri=10.10.1.1:31860
shard.backup_uri=10.10.1.2:31860
shard.num=2
shard.id=0
...
```
```bash
# shard1.prop
shard.server_uri=10.10.1.3:31860
shard.backup_uri=10.10.1.4:31860
shard.num=2
shard.id=1
```
To start each shard machine:
```bash
# shard 0 primary
sudo shardsvr -P shard0.prop -p leader=true
# shard 0 backup
sudo shardsvr -P shard0.prop -p shard.server_uri=10.10.1.2:31860
# shard 1 primary
sudo shardsvr -P shard1.prop -p leader=true
# shard 1 backup
sudo shardsvr -P shard1.prop -p shard.server_uri=10.10.1.4:31860
```
