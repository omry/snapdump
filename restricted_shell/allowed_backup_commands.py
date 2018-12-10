#!/usr/local/bin/python

import os
import re
import sys
import subprocess

datasets = [
  'storage/home',
  'storage/datasets01',
]

def denied():
  print("Access denied")
  sys.exit(1)

if 'SSH_ORIGINAL_COMMAND' not in os.environ:
  denied()

cmd = os.environ['SSH_ORIGINAL_COMMAND']

# read only, can take arbitrary flags
re_list = re.compile('^zfs list( -\w( [\w/_-]+)?)*$')

# work 	exactly on one snapshot and does not take any additional flags
re_snap_ops = re.compile('^zfs (snapshot|destroy) ([\w/]+)@([\w]+)$')
re_send = re.compile('^zfs send ([\w/]+)@([\w]+)( -i [\w]+)?$')
re_recv = re.compile('^zfs recv -F ([\w/]+)$')

def unsupported_dataset_error(dataset):
  print(f"{dataset} is not in the list of managed datasets, fix in {__file__} in the server")
  sys.exit(1)

def exec(cmd):
  subprocess.call(cmd, shell = True)

if re_list.match(cmd):
  exec(cmd)
elif re_snap_ops.match(cmd):
  m = re_snap_ops.match(cmd)
  op = m.group(1)
  dataset = m.group(2)
  snapshot = m.group(3)
  if dataset in datasets:
    exec(cmd)
  else:
    unsupported_dataset_error(dataset)
elif re_send.match(cmd):
  m = re_send.match(cmd)
  dataset = m.group(1)
  if dataset in datasets:
    exec(cmd)
  else:
    unsupported_dataset_error(dataset)
elif re_recv.match(cmd):
  # no need to verify dataset because we are creatign a new one
  # if dataset is already exist we will have an error, and since we
  # do not allow arbitrary flags the client can't force
  exec(cmd)
else:
  denied()
