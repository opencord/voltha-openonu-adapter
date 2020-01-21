#!/bin/bash

set -x
cat BRCM-BVM4K00BRA0915-0083-5023_020O02414.json | etcdctl --endpoints=172.17.0.1:2379 put service/voltha/omci_mibs/templates/BRCM/BVM4K00BRA0915-0083/5023_020O02414
cat ALCL-BVMGR00BRAXS020XA-3FE47059BFHB52.json | etcdctl --endpoints=172.17.0.1:2379 put service/voltha/omci_mibs/templates/ALCL/BVMGR00BRAXS020XA/3FE47059BFHB52

