[![PyPI version](https://badge.fury.io/py/snapdump.svg)](https://badge.fury.io/py/snapdump)

# Snapdump

This tool is primarily intended to be used to backup zfs servers to foreign file systems with the offsite backup use case in mind.
It's intended to be ran from outside the ZFS box, and uses passwordless ssh login to perform zfs operations.

## Installation
```
$ pip install snapdump
$ snapdump  --help
usage: snapdump [-h] [--conf CONF] {backup,restore,list,cleanup} ...

snapdump : backup and restore zfs snapshots to/from a foreign file system

positional arguments:
  {backup,restore,list,cleanup}
                        sub-command help
    backup              Backup
    restore             Restore
    list                Restore
    cleanup             Cleanup old snapshots and dump directories

optional arguments:
  -h, --help            show this help message and exit
  --conf CONF, -c CONF  Config file name
```
You will need to start by creating a config file, see [config.sample.yml](snapdump/config.sample.yml) for an example.

in addition, you need password-less ssh root access to your server. See [restricted_shell/README.md](restricted_shell/README.md) for details about improving security.

## Features
* Incremental snapshot dump and restore
* Taking zfs snapshots automatically
* Automatic cleanup of both ZFS znapshots and dumped files

Script is intended to be executed from a cron job, at a high frequency. it will not do anything 
if the correct interval has not passed.

## Commands
### backup
backup will create full or incremental snapshots of each dataset mentioend in the config, and will also perform cleanup

```
$ snapdump -c /path-to-config/config.yml backup
Creating incremental snapshot dump for storage/home@2018_12_14__00_23_58 based on 2018_12_14__00_21_47
Creating incremental snapshot dump for storage/datasets01@2018_12_14__00_23_58 based on 2018_12_14__00_21_47
```

### list
Listing all current snapshots per dataset.
```
snapdump -c /path-to-config/config.yml list
storage/home:
	= storage/home@2018_12_10__19_20_34
	+ storage/home@2018_12_14__00_21_47
	+ storage/home@2018_12_14__00_23_58
storage/datasets01:
	= storage/datasets01@2018_12_11__04_47_33
	+ storage/datasets01@2018_12_14__00_21_47
	+ storage/datasets01@2018_12_14__00_23_58
```
Each snapshot is prefixed with = or +, to indicate if it's a full (=) or incremental (+) snapshot.

### restore
Restore will take a snapshot name and optionally a destination dataset and restore it. it will work correctly for incremental snapshots as well.
if destination dataset name is not provided, a new dataset with the suffix _restore will be created.

```
$ snapdump -c /path-to-config/config.yml restore -s storage/datasets01@2018_12_14__00_23_58 
Restoring snapshot storage/datasets01@2018_12_14__00_23_58 to storage/datasets01_restore
```

### cleanup
Initiate the cleanup, this is not normally needed because backup is cleaning up automatically
