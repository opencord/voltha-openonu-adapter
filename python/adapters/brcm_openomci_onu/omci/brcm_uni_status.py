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
#

from __future__ import absolute_import
import structlog
from pyvoltha.adapters.extensions.omci.tasks.task import Task
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, failure, returnValue, DeferredQueue
from pyvoltha.adapters.extensions.omci.omci_defs import ReasonCodes, EntityOperations
from pyvoltha.adapters.extensions.omci.omci_me import OntGFrame
from pyvoltha.adapters.extensions.omci.omci_me import PptpEthernetUniFrame, VeipUniFrame
from voltha_protos.extensions_pb2 import SingleGetValueResponse, GetValueResponse, GetOnuUniInfoResponse

RC = ReasonCodes
OP = EntityOperations


class BrcmUniStatusTask(Task):
    """
    Get the status of the UNI Port
    """
    task_priority = Task.DEFAULT_PRIORITY + 20
    name = "Broadcom UNI Status Task"

    def __init__(self, omci_agent, device_id, get_val_req, entity_id, msg_queue, priority=task_priority):
        """
        Class initialization

        :param omci_agent: (OmciAdapterAgent) OMCI Adapter agent
        :param device_id: (str) ONU Device ID
        :param get_val_req: (voltha_pb2.SingleGetValueResponse)
        :param priority: (int) OpenOMCI Task priority (0..255) 255 is the highest
        """
        super(BrcmUniStatusTask, self).__init__(BrcmUniStatusTask.name,
                                              omci_agent,
                                              device_id,
                                              priority=priority,
                                              exclusive=True)

        self.log = structlog.get_logger(device_id=device_id,
                                        name=BrcmUniStatusTask.name,
                                        task_id=self._task_id)

        self._device = omci_agent.get_device(device_id)
        self._req = get_val_req
        self._results = SingleGetValueResponse()
        self._local_deferred = None
        self._config = self._device.configuration
        self._entity_id = entity_id
        self.uni_status_response_queue = msg_queue

        self.log.info("get-uni-staus-deviceid",deviceid=device_id)

    def cancel_deferred(self):
        super(BrcmUniStatusTask, self).cancel_deferred()

        d, self._local_deferred = self._local_deferred, None
        try:
            if d is not None and not d.called:
                d.cancel()
        except:
            pass

    def start(self):
        """
        Start UNI/PPTP Get Status Task
        """
        super(BrcmUniStatusTask, self).start()
        self._local_deferred = reactor.callLater(0, self.perform_get_uni_status)
    def stop(self):
         self.cancel_deferred()
         super(BrcmUniStatusTask, self).stop()


    @inlineCallbacks
    def perform_get_uni_status(self):
        """
        Perform the Get UNI Status
        """
        self.log.info('get-uni-status-uni-index-is  ',uni_index=self._req.uniInfo.uniIndex)


        try:
            pptp_list = sorted(self._config.pptp_entities) if self._config.pptp_entities else []
            pptp_items = ['administrative_state', 'operational_state', 'config_ind', 'max_frame_size', 'sensed_type', 'bridged_ip_ind']
            for entity_id in pptp_list:
                self.log.info('entity-id',entity_id)
            msg = PptpEthernetUniFrame(self._entity_id, attributes=pptp_items)
            yield self._send_omci_msg(msg)
        except Exception as e:
            self._results.response.status = GetValueResponse.ERROR
            self._results.response.errReason = GetValueResponse.REASON_UNDEFINED
            self.log.exception('get-uni-status', e=e)
        finally:
            self.log.info('uni-status-response',self._results)
            yield self.uni_status_response_queue.put(self._results)

    @inlineCallbacks
    def _send_omci_msg(self, me_message):
        frame = me_message.get()
        results = yield self._device.omci_cc.send(frame)
        if self._check_status_and_state(results, 'get-uni-status'):
           omci_msg = results.fields['omci_message'].fields
           self._results.response.status = GetValueResponse.OK
           self._collect_uni_admin_state(results)
           self._collect_uni_operational_state(results)
           self._collect_uni_config_ind(results)
        else:
           self._results.response.status = GetValueResponse.ERROR
           self._results.response.errReason = GetValueResponse.UNSUPPORTED

    def _collect_uni_admin_state(self, results):
        omci_msg = results.fields['omci_message'].fields
        admin_state = omci_msg.get("data").get("administrative_state")
        if admin_state == 0 :
           self._results.response.uniInfo.admState  = GetOnuUniInfoResponse.UNLOCKED
        elif admin_state == 1 :
           self._results.response.uniInfo.admState  = GetOnuUniInfoResponse.LOCKED
        else:
           self._results.response.uniInfo.admState  = GetOnuUniInfoResponse.ADMSTATE_UNDEFINED

    def _collect_uni_operational_state(self, results):
        self.log.info('collect-uni-oper-state')
        omci_msg = results.fields['omci_message'].fields
        oper_state=omci_msg.get("data").get("operational_state")
        if oper_state == 0 :
           self._results.response.uniInfo.operState = GetOnuUniInfoResponse.ENABLED
        elif oper_state == 1 :
           self._results.response.uniInfo.operState = GetOnuUniInfoResponse.DISABLED
        else:
           self._results.response.uniInfo.operState = GetOnuUniInfoResponse.OPERSTATE_UNDEFINED

    def _collect_uni_config_ind(self, results):
        config_ind_map = {0:GetOnuUniInfoResponse.UNKOWN,
                          1:GetOnuUniInfoResponse.TEN_BASE_T_FDX,
                          2:GetOnuUniInfoResponse.HUNDRED_BASE_T_FDX,
                          3:GetOnuUniInfoResponse.GIGABIT_ETHERNET_FDX,
                          4:GetOnuUniInfoResponse.TEN_G_ETHERNET_FDX,
                          17:GetOnuUniInfoResponse.TEN_BASE_T_HDX,
                          18:GetOnuUniInfoResponse.HUNDRED_BASE_T_HDX,
                          19:GetOnuUniInfoResponse.GIGABIT_ETHERNET_HDX,
                          }
        self.log.info('collect-config-ind')
        omci_msg = results.fields['omci_message'].fields
        config_ind =omci_msg.get("data").get("config_ind", GetOnuUniInfoResponse.UNKOWN)
        self._results.response.uniInfo.configInd  = config_ind_map.get(config_ind, GetOnuUniInfoResponse.UNKOWN)

    def _check_status_and_state(self, results, operation=''):
        """
        Check the results of an OMCI response.  An exception is thrown
        if the task was cancelled or an error was detected.

        :param results: (OmciFrame) OMCI Response frame
        :param operation: (str) what operation was being performed
        :return: True if successful, False if the entity existed (already created)
        """
        omci_msg = results.fields['omci_message'].fields
        status = omci_msg['success_code']
        error_mask = omci_msg.get('parameter_error_attributes_mask', 'n/a')
        failed_mask = omci_msg.get('failed_attributes_mask', 'n/a')
        unsupported_mask = omci_msg.get('unsupported_attributes_mask', 'n/a')

        self.log.debug("omci-response", operation=operation,
                       omci_msg=omci_msg, status=status,
                       error_mask=error_mask, failed_mask=failed_mask,
                       unsupported_mask=unsupported_mask)

        self.strobe_watchdog()
        if status == RC.Success:
            return True
        else:
           self.log.info("omci-reponse-failed",  status, "error-mask-is",
           error_mask, "failed-mask-is ", failed_mask, "unsupported-mask-is ", unsupported_mask)
           return False

