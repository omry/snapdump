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

### restore
Restore will take a snapshot name and optionally a destination dataset and restore it. it will work correctly for incremental snapshots as well.
if destination dataset name is not provided, a new dataset with the suffix _restore will be created.

### list
Listing all current snapshots per dataset.
```
$ ./snapdump.py list
storage/datasets01:
        = storage/datasets01@2018_12_10__19_15_31
storage/home:
        = storage/home@2018_12_09__19_20_34
        + storage/home@2018_12_10__19_20_04
```
Each snapshot is prefixed with = or +, to indicate if it's a full or incremental snapshot.
### cleanup
Initiate the cleanup, this is not normally needed because back is cleaning up automatically
