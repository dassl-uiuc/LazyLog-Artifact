# Data Log Design
* Clients connect to the data log using the DataLogClient. 
* Primary datalog connects to the backup using the DataLogClient. 
* Flow
    1) Clients send, `<cid, csn, data payload>` (assume also replicated at backup). Store on per-client memory region somewhere. Add to data map from `<cid, csn>` to `<data region pointer>` 
    2) MM sends vector of `<cid, csn, gsn>`'s
    3) For each `<cid, csn, gsn>` in list, 
        If `<cid, csn>` is in data map, write `<gsn, data payload>` to disk and to replication vector, free memory region for payload (could just use as a userspace cache instead)
        Else, wait until you get the entry (maybe with timeout for future versions, if timedout, consider failed, write `<gsn, no op>` to disk, and to replication vector).   
    4) Replicate vector to backup. Backup also frees associated memory if it has it before writing to disk.
    5) Acknowledge MM. 