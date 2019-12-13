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
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, TimeoutError, failure
from pyvoltha.adapters.extensions.omci.omci_me import Ont2G, OmciNullPointer, PriorityQueueFrame, \
    Ieee8021pMapperServiceProfileFrame, MacBridgePortConfigurationDataFrame
from pyvoltha.adapters.extensions.omci.tasks.task import Task
from pyvoltha.adapters.extensions.omci.omci_defs import EntityOperations, ReasonCodes
from pyvoltha.adapters.extensions.omci.omci_entities import OntG, Tcont, PriorityQueueG

OP = EntityOperations
RC = ReasonCodes

# The DELETE_TP_TASK_PRIORITY should be higher than SETUP_TP_TASK_PRIORITY
DELETE_TP_TASK_PRIORITY = 250


class TechProfileDeleteFailure(Exception):
    """
    This error is raised by default when the delete fails
    """


class TechProfileResourcesFailure(Exception):
    """
    This error is raised by when one or more resources required is not available
    """


class BrcmTpDeleteTask(Task):
    """
    OpenOMCI Tech-Profile Delete Task

    """

    name = "Broadcom Tech-Profile Delete Task"

    def __init__(self, omci_agent, handler, uni_id, tp_table_id,
                 tcont=None, gem_port=None):
        """
        Class initialization

        :param omci_agent: (OmciAdapterAgent) OMCI Adapter agent
        :param handler: (BrcmOpenomciOnuHandler) ONU Device Handler Instance
        :param uni_id: (int) numeric id of the uni port on the onu device, starts at 0
        :param tp_table_id: (int) Technology Profile Table-ID
        """
        log = structlog.get_logger(device_id=handler.device_id, uni_id=uni_id)
        log.debug('function-entry')

        super(BrcmTpDeleteTask, self).__init__(BrcmTpDeleteTask.name,
                                               omci_agent,
                                               handler.device_id,
                                               priority=DELETE_TP_TASK_PRIORITY,
                                               exclusive=True)

        self.log = log

        self._onu_device = omci_agent.get_device(handler.device_id)
        self._local_deferred = None

        self._uni_port = handler.uni_ports[uni_id]
        assert self._uni_port.uni_id == uni_id

        # Tech Profile Table ID
        self._tp_table_id = tp_table_id

        self._tcont = tcont
        self._gem_port = gem_port

        self._alloc_id = self._tcont.alloc_id if self._tcont else None
        self._gem_port_id = self._gem_port.gem_id if gem_port else None

        # Entity IDs. IDs with values can probably be most anything for most ONUs,
        #             IDs set to None are discovered/set
        self._mac_bridge_service_profile_entity_id = \
            handler.mac_bridge_service_profile_entity_id
        self._ieee_mapper_service_profile_entity_id = \
            handler.pon_port.ieee_mapper_service_profile_entity_id
        self._mac_bridge_port_ani_entity_id = \
            handler.pon_port.mac_bridge_port_ani_entity_id

    def cancel_deferred(self):
        self.log.debug('function-entry')
        super(BrcmTpDeleteTask, self).cancel_deferred()

        d, self._local_deferred = self._local_deferred, None
        try:
            if d is not None and not d.called:
                d.cancel()
        except:
            pass

    def start(self):
        """
        Start the Tech-Profile Delete
        """
        self.log.debug('function-entry')
        super(BrcmTpDeleteTask, self).start()
        if self._tcont is not None:
            self._local_deferred = reactor.callLater(0, self.delete_tcont_and_associated_me)
        elif self._gem_port is not None:
            self._local_deferred = reactor.callLater(0, self.delete_gem_port_nw_ctp_and_associated_me)
        else:
            raise Exception("both-alloc-id-and-gem-port-id-are-none")

    def stop(self):
        """
        Shutdown Tech-Profile delete tasks
        """
        self.log.debug('function-entry')
        self.log.debug('stopping')

        self.cancel_deferred()
        super(BrcmTpDeleteTask, self).stop()

    def check_status_and_state(self, results, operation=''):
        """
        Check the results of an OMCI response.  An exception is thrown
        if the task was cancelled or an error was detected.

        :param results: (OmciFrame) OMCI Response frame
        :param operation: (str) what operation was being performed
        :return: True if successful, False if the entity existed (already created)
        """
        self.log.debug('function-entry')

        omci_msg = results.fields['omci_message'].fields
        status = omci_msg['success_code']
        error_mask = omci_msg.get('parameter_error_attributes_mask', 'n/a')
        failed_mask = omci_msg.get('failed_attributes_mask', 'n/a')
        unsupported_mask = omci_msg.get('unsupported_attributes_mask', 'n/a')

        self.log.debug("OMCI Result", operation=operation, omci_msg=omci_msg, status=status, error_mask=error_mask,
                       failed_mask=failed_mask, unsupported_mask=unsupported_mask)

        if status == RC.Success:
            self.strobe_watchdog()
            return True

        elif status == RC.InstanceExists:
            return False

        raise TechProfileDeleteFailure(
            '{} failed with a status of {}, error_mask: {}, failed_mask: {}, '
            'unsupported_mask: {}'.format(operation, status, error_mask, failed_mask, unsupported_mask))

    @inlineCallbacks
    def delete_tcont_and_associated_me(self):
        self.log.debug('function-entry')

        omci_cc = self._onu_device.omci_cc

        try:
            ################################################################################
            # Reset TCONT ME
            ################################################################################

            tcont_idents = self._onu_device.query_mib(Tcont.class_id)
            self.log.debug('tcont-idents', tcont_idents=tcont_idents)
            tcont_entity_id = None
            for k, v in tcont_idents.items():
                if not isinstance(v, dict):
                    continue
                alloc_check = v.get('attributes', {}).get('alloc_id', 0)
                # Some onu report both to indicate an available tcont
                if alloc_check == self._alloc_id:
                    tcont_entity_id = k
                    break

            if tcont_entity_id is None:
                self.log.error("tcont-not-found-for-delete", alloc_id=self._alloc_id)
                return

            self.log.debug('found-tcont', tcont_entity_id=tcont_entity_id, alloc_id=self._alloc_id)

            # Remove the TCONT (rather reset the TCONT to point alloc_id to 0xffff)
            # The _tcont.remove_from_hardware is already doing check_status_and_state
            yield self._tcont.remove_from_hardware(omci_cc)

            # At this point, the gem-ports should have been removed already.
            # Remove the 8021p Mapper and ANI MacBridgePortConfigData
            yield self._delete__8021p_mapper__ani_mac_bridge_port()

            # There are no other associated MEs now.
            # There might be TrafficScheduler MEs that point to a TCONT.
            # TrafficScheduler ME not used currently.

            self.deferred.callback("tech-profile-remove-success--tcont")

        except TimeoutError as e:
            self.log.warn('rx-timeout-tech-profile', e=e)
            self.deferred.errback(failure.Failure(e))

        except Exception as e:
            self.log.exception('omci-delete-tech-profile', e=e)
            self.deferred.errback(failure.Failure(e))

    @inlineCallbacks
    def delete_gem_port_nw_ctp_and_associated_me(self):
        omci_cc = self._onu_device.omci_cc
        try:
            ################################################################################
            # Delete GemPortNetworkCTP and GemPortInterworkingPoint
            ################################################################################
            # The _gem_port.remove_from_hardware is already doing check_status_and_state
            yield self._gem_port.remove_from_hardware(omci_cc)

            self.deferred.callback("tech-profile-remove-success--gem-port")

        except TimeoutError as e:
            self.log.warn('rx-timeout-tech-profile', e=e)
            self.deferred.errback(failure.Failure(e))

        except Exception as e:
            self.log.exception('omci-delete-tech-profile', e=e)
            self.deferred.errback(failure.Failure(e))

    @inlineCallbacks
    def _delete__8021p_mapper__ani_mac_bridge_port(self):

        omci_cc = self._onu_device.omci_cc

        try:
            ################################################################################
            # Delete 8021p mapper
            ################################################################################
            msg = Ieee8021pMapperServiceProfileFrame(
                self._ieee_mapper_service_profile_entity_id +
                self._uni_port.mac_bridge_port_num +
                self._tp_table_id
            )
            frame = msg.delete()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'delete-8021p-mapper-service-profile')

            ################################################################################
            # Delete MacBridgePortConfigData
            ################################################################################
            # TODO: magic. make a static variable for tp_type
            msg = MacBridgePortConfigurationDataFrame(
                self._mac_bridge_port_ani_entity_id + self._uni_port.entity_id + self._tp_table_id,  # Entity ID
            )
            frame = msg.delete()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'delete-mac-bridge-port-configuration-data-8021p-mapper')

        except TimeoutError as e:
            self.log.warn('rx-timeout-8021p-ani-port-delete', e=e)
            raise

        except Exception as e:
            self.log.exception('omci-delete-8021p-ani-port-delete', e=e)
            raise
