#!/bin/bash
#
# Copyright 2020 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -x
cat ALCL-BVMGR00BRAXS020XA-3FE47059BFHB52-v2.json | etcdctl --endpoints=172.17.0.1:2379 put service/voltha/omci_mibs/templates/ALCL/BVMGR00BRAXS020XA/3FE47059BFHB52
cat ALCL-BVMGR00BRAXS020XA-3FE47059BFHB21-v2.json | etcdctl --endpoints=172.17.0.1:2379 put service/voltha/omci_mibs/templates/ALCL/BVMGR00BRAXS020XA/3FE47059BFHB21
cat BRCM-BVM4K00BRA0915-0083-5023_020O02414-v2.json | etcdctl --endpoints=172.17.0.1:2379 put service/voltha/omci_mibs/templates/BRCM/BVM4K00BRA0915-0083/5023_020O02414
cat BRCM-BVM4K00BRA0915-0083-5023_003GWOV36-VEIP-v1.json | etcdctl --endpoints=172.17.0.1:2379 put service/voltha/omci_mibs/templates/BRCM/BVM4K00BRA0915-0083/5023_003GWOV36
cat BBSM-12345123451234512345-00000000000001-v1.json | etcdctl --endpoints=172.17.0.1:2379 put service/voltha/omci_mibs/templates/BBSM/12345123451234512345/00000000000001

