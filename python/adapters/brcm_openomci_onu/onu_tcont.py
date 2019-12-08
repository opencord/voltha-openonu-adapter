#
# Copyright 2018 the original author or authors.
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
import structlog
from twisted.internet.defer import inlineCallbacks, returnValue
from pyvoltha.adapters.extensions.omci.omci_me import TcontFrame
from pyvoltha.adapters.extensions.omci.omci_defs import ReasonCodes
from pyvoltha.adapters.extensions.omci.onu_configuration import OMCCVersion

RC = ReasonCodes


class OnuTCont(object):
    G988_OMCC_VERSIONS = [OMCCVersion.G_988_2010_Base,
                          OMCCVersion.G_988_2011_Amd_1_Base,
                          OMCCVersion.G_988_2012_Amd_2_Base,
                          OMCCVersion.G_988_2012_Base,
                          OMCCVersion.G_988_2010,
                          OMCCVersion.G_988_2011_Amd_1,
                          OMCCVersion.G_988_2012_Amd_2,
                          OMCCVersion.G_988_2012,
                          OMCCVersion.G_988_2014_Amd_1]
    """
    Broadcom ONU specific implementation
    """

    def __init__(self, handler, uni_id, alloc_id, q_sched_policy):

        self.log = structlog.get_logger(device_id=handler.device_id, uni_id=uni_id, alloc_id=alloc_id)

        self.uni_id = uni_id
        self.alloc_id = alloc_id
        self._q_sched_policy = 0
        self.q_sched_policy = q_sched_policy

        self._handler = handler
        self._entity_id = None

    def __str__(self):
        return "OnuTCont - uni_id: {}, entity_id: {}, alloc-id: {}, q_sched_policy: {}".format(
            self.uni_id, self._entity_id, self.alloc_id, self.q_sched_policy)

    def __repr__(self):
        return str(self)

    @property
    def entity_id(self):
        return self._entity_id

    @entity_id.setter
    def entity_id(self, value):
        self._entity_id = value

    @property
    def q_sched_policy(self):
        return self._q_sched_policy

    @q_sched_policy.setter
    def q_sched_policy(self, q_sched_policy):
        sp = ('Null', 'WRR', 'StrictPriority')
        if q_sched_policy in sp:
            self._q_sched_policy = sp.index(q_sched_policy)
        else:
            self._q_sched_policy = 0

    @staticmethod
    def create(handler, tcont):

        return OnuTCont(handler,
                        tcont['uni_id'],
                        tcont['alloc-id'],
                        tcont['q_sched_policy']
                        )

    @inlineCallbacks
    def add_to_hardware(self, omci, tcont_entity_id):
        self.log.debug('add-to-hardware', tcont_entity_id=tcont_entity_id)

        self._entity_id = tcont_entity_id

        try:
            # FIXME: self.q_sched_policy seems to be READ-ONLY
            # Ideally the READ-ONLY or NOT attribute is available from ONU-2G ME
            # msg = TcontFrame(self.entity_id, self.alloc_id, self.q_sched_policy)
            msg = TcontFrame(self.entity_id, self.alloc_id)
            frame = msg.set()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci.send(frame)
            self.check_status_and_state(results, 'set-tcont')

        except Exception as e:
            self.log.exception('tcont-set', e=e)
            raise

        returnValue(results)

    @inlineCallbacks
    def remove_from_hardware(self, omci):
        self.log.debug('remove-from-hardware', tcont_entity_id=self.entity_id)

        # Release tcont by setting alloc_id=0xFFFF
        # TODO: magic number, create a named variable

        try:
            initial_alloc_id_value = 0xFF

            onu_device = self._handler.onu_omci_device
            omcc_version = onu_device.configuration.omcc_version
            if omcc_version in OnuTCont.G988_OMCC_VERSIONS:
                initial_alloc_id_value = 0xFFFF

            msg = TcontFrame(self.entity_id, initial_alloc_id_value)
            frame = msg.set()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci.send(frame)
            self.check_status_and_state(results, 'delete-tcont')
        except Exception as e:
            self.log.exception('tcont-delete', e=e)
            raise

        returnValue(results)

    def check_status_and_state(self, results, operation=''):
        omci_msg = results.fields['omci_message'].fields
        status = omci_msg['success_code']
        error_mask = omci_msg.get('parameter_error_attributes_mask', 'n/a')
        failed_mask = omci_msg.get('failed_attributes_mask', 'n/a')
        unsupported_mask = omci_msg.get('unsupported_attributes_mask', 'n/a')

        self.log.debug("OMCI Result", operation=operation, omci_msg=omci_msg,
                       status=status, error_mask=error_mask,
                       failed_mask=failed_mask, unsupported_mask=unsupported_mask)

        if status == RC.Success:
            return True

        elif status == RC.InstanceExists:
            return False
