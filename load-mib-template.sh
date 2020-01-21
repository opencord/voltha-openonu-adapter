#!/bin/bash

set -x
cat BRCM-BVM4K00BRA0915-0083-5023_020O02414.json | etcdctl --endpoints=172.17.0.1:2379 put service/voltha/omci_mibs/templates/BRCM/BVM4K00BRA0915-0083/5023_020O02414
etcdctl --endpoints=172.17.0.1:2379 get --print-value-only service/voltha/omci_mibs/templates/BRCM/BVM4K00BRA0915-0083/5023_020O02414 | jq '.'

