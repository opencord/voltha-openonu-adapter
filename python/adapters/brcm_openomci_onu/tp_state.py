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

from __future__ import absolute_import

import random
import structlog
from twisted.internet import reactor


class TpState:
    ALLOC_ID = "alloc_id"
    GEM_ID = "gem_id"

    def __init__(self, handler, uni_id, tp_path):
        self.log = structlog.get_logger(device_id=handler.device_id, uni_id=uni_id, tp_path=tp_path)
        self._handler = handler
        self._uni_id = uni_id
        self._tp_path = tp_path
        self._tp_task_ref = None
        self._tp_setup_done = False
        # When the vlan filter is being removed for a given TP ID on a given UNI,
        # mark that we are expecting a tp delete to happen for this UNI.
        # Unless the TP delete is complete to not allow new vlan add tasks to this TP ID
        self._is_tp_delete_pending = False
        # Map maintains details of PON resources (alloc_id and gem_port_id(s) to be deleted) for a given TP
        self._pending_delete_pon_res_map = dict()

    @property
    def tp_task_ref(self):
        return self._tp_task_ref

    @tp_task_ref.setter
    def tp_task_ref(self, tp_task_ref):
        self._tp_task_ref = tp_task_ref

    @property
    def tp_setup_done(self):
        return self._tp_setup_done

    @tp_setup_done.setter
    def tp_setup_done(self, tp_setup_done):
        self._tp_setup_done = tp_setup_done

    @property
    def is_tp_delete_pending(self):
        return self._is_tp_delete_pending

    @is_tp_delete_pending.setter
    def is_tp_delete_pending(self, is_tp_delete_pending):
        self._is_tp_delete_pending = is_tp_delete_pending

    def queue_pending_delete_pon_resource(self, res_type, res):
        if res_type not in self._pending_delete_pon_res_map:
            if res_type == TpState.ALLOC_ID:
                # There is only one alloc-id for a TP
                self._pending_delete_pon_res_map[TpState.ALLOC_ID] = res
            elif res_type == TpState.GEM_ID:
                # There can be more than one gem-port-id for a TP
                self._pending_delete_pon_res_map[TpState.GEM_ID] = list()
                self._pending_delete_pon_res_map[TpState.GEM_ID].append(res)
            else:
                self.log.error("unknown-res-type", res_type=res_type)
        else:
            if res_type == TpState.ALLOC_ID:
                self.log.warn("alloc-id-already-pending-for-deletion", alloc_id=res)
            elif res_type == TpState.GEM_ID:
                # Make sure that we are not adding duplicate gem-port-id to the list
                for v in self._pending_delete_pon_res_map[TpState.GEM_ID]:
                    if v.gem_id == res.gem_id:
                        self.log.warn("gem-id-already-pending-for-deletion", gem_id=res.gem_id)
                        return
                self._pending_delete_pon_res_map[TpState.GEM_ID].append(res)
            else:
                self.log.error("unknown-res-type", res_type=res_type)

    def pon_resource_delete_complete(self, res_type, res_id):
        if res_type not in self._pending_delete_pon_res_map:
            self.log.error("resource-was-not-queued-for-delete", res_type=res_type, res_id=res_id)
            return
        if res_type == TpState.ALLOC_ID:
            # After removing the TCONT, remove the ALLOC_ID key
            del self._pending_delete_pon_res_map[res_type]
        else:
            for v in self._pending_delete_pon_res_map[TpState.GEM_ID]:
                if v.gem_id == res_id:
                    self._pending_delete_pon_res_map[TpState.GEM_ID].remove(v)
                    if len(self._pending_delete_pon_res_map[TpState.GEM_ID]) == 0:
                        del self._pending_delete_pon_res_map[TpState.GEM_ID]
                    return
            self.log.warn("gem-id-was-not-queued-for-delete", gem_id=res_id)

    def get_queued_resource_for_delete(self, res_type, res_id):
        if res_type not in self._pending_delete_pon_res_map:
            self.log.warn("resource-was-not-queued-for-delete", res_type=res_type, res_id=res_id)
            return None
        if res_type == TpState.ALLOC_ID:
            # After removing the TCONT, remove the ALLOC_ID key
            return self._pending_delete_pon_res_map[res_type]
        elif res_type == TpState.GEM_ID:
            for i, v in enumerate(self._pending_delete_pon_res_map[res_type]):
                if v.gem_id == res_id:
                    return self._pending_delete_pon_res_map[res_type][i]
        return None

    def is_all_pon_resource_delete_complete(self):
        return len(self._pending_delete_pon_res_map) == 0

    def reset_tp_state(self):
        self._tp_task_ref = None
        self._tp_setup_done = False
        self._is_tp_delete_pending = False
        self._pending_delete_pon_res_map.clear()
        self.log.info("reset-tp-success")
