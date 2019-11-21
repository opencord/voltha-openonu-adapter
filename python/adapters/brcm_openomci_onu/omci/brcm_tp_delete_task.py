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

import structlog
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, TimeoutError, failure
from pyvoltha.adapters.extensions.omci.omci_me import *
from pyvoltha.adapters.extensions.omci.tasks.task import Task
from pyvoltha.adapters.extensions.omci.omci_defs import *
from pyvoltha.adapters.extensions.omci.omci_entities import *
from adapters.brcm_openomci_onu.uni_port import *
from adapters.brcm_openomci_onu.pon_port import TASK_PRIORITY, DEFAULT_GEM_PAYLOAD

OP = EntityOperations
RC = ReasonCodes


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
                 alloc_id=None, gem_port_id=None):
        """
        Class initialization

        :param omci_agent: (OmciAdapterAgent) OMCI Adapter agent
        :param handler: (BrcmOpenomciOnuHandler) ONU Device Handler Instance
        :param uni_id: (int) numeric id of the uni port on the onu device, starts at 0
        """
        log = structlog.get_logger(device_id=handler.device_id, uni_id=uni_id)
        log.debug('function-entry')

        super(BrcmTpDeleteTask, self).__init__(BrcmTpDeleteTask.name,
                                               omci_agent,
                                               handler.device_id,
                                               priority=TASK_PRIORITY,
                                               exclusive=True)

        self.log = log

        self._onu_device = omci_agent.get_device(handler.device_id)
        self._local_deferred = None

        self._uni_port = handler.uni_ports[uni_id]
        assert self._uni_port.uni_id == uni_id

        # Tech Profile Table ID
        self._tp_table_id = tp_table_id

        self._alloc_id = alloc_id
        self._gem_port_id = gem_port_id

        # Entity IDs. IDs with values can probably be most anything for most ONUs,
        #             IDs set to None are discovered/set
        self._mac_bridge_service_profile_entity_id = \
            handler.mac_bridge_service_profile_entity_id
        self._ieee_mapper_service_profile_entity_id = \
            handler.pon_port.ieee_mapper_service_profile_entity_id
        self._mac_bridge_port_ani_entity_id = \
            handler.pon_port.mac_bridge_port_ani_entity_id
        self._gal_enet_profile_entity_id = \
            handler.gal_enet_profile_entity_id

        # Extract the current set of TCONT and GEM Ports from the Handler's pon_port that are
        # relevant to this task's UNI. It won't change. But, the underlying pon_port may change
        # due to additional tasks on different UNIs. So, it we cannot use the pon_port affter
        # this initializer
        self._tcont = None
        for tcont in handler.pon_port.tconts.itervalues():
            if tcont.uni_id is not None and tcont.uni_id != self._uni_port.uni_id:
                continue
            if tcont.alloc_id == self._alloc_id:
                self._tcont = tcont
                break

        self._gem_port = None
        for gem_port in handler.pon_port.gem_ports.itervalues():
            if gem_port.uni_id is not None and gem_port.uni_id != self._uni_port.uni_id:
                continue
            if gem_port.gem_id == self._gem_port_id:
                self._gem_port = gem_port
                break

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
        if self._alloc_id is not None:
            self._local_deferred = reactor.callLater(0, self.delete_tcont_and_associated_me)
        elif self._gem_port_id is not None:
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
            '{} failed with a status of {}, error_mask: {}, failed_mask: {}, unsupported_mask: {}'
                .format(operation, status, error_mask, failed_mask, unsupported_mask))

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
            results = yield self._tcont.remove_from_hardware(omci_cc)
            self.check_status_and_state(results, 'tcont-deleted')

            # At this point, the gem-ports should have been removed already.
            # Remove the 8021p Mapper and ANI MacBridgePortConfigData
            self._delete__8021pMapper__ani_mac_bridge_port()

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
            results = self._gem_port.remove_from_hardware(omci_cc)
            self.check_status_and_state(results, 'gem-port-deleted')

            ################################################################################
            # Delete GemPortNetworkCTP and GemPortInterworkingPoint
            ################################################################################

            gem_entity_ids = [OmciNullPointer] * 8
            for gem_port in self._gem_ports:
                self.log.debug("tp-gem-port", entity_id=gem_port.entity_id, uni_id=gem_port.uni_id)

                if gem_port.direction == "upstream" or \
                        gem_port.direction == "bi-directional":
                    for i, p in enumerate(reversed(gem_port.pbit_map)):
                        if p == '1':
                            gem_entity_ids[i] = gem_port.entity_id
                elif gem_port.direction == "downstream":
                    # Downstream gem port p-bit mapper is inverse of upstream
                    # TODO: Could also be a case of multicast. Not supported for now
                    pass

            msg = Ieee8021pMapperServiceProfileFrame(
                self._ieee_mapper_service_profile_entity_id + self._uni_port.mac_bridge_port_num,
                # 802.1p mapper Service Mapper Profile ID
                interwork_tp_pointers=gem_entity_ids  # Interworking TP IDs
            )
            frame = msg.set()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'set-8021p-mapper-service-profile-ul')

            self.deferred.callback("tech-profile-remove-success--gem-port")

        except TimeoutError as e:
            self.log.warn('rx-timeout-tech-profile', e=e)
            self.deferred.errback(failure.Failure(e))

        except Exception as e:
            self.log.exception('omci-delete-tech-profile', e=e)
            self.deferred.errback(failure.Failure(e))

    @inlineCallbacks
    def _delete__8021pMapper__ani_mac_bridge_port(self):

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
                self._mac_bridge_port_ani_entity_id + self._uni_port.mac_bridge_port_num + self._tp_table_id,
                bridge_id_pointer=(
                        self._mac_bridge_service_profile_entity_id +
                        self._uni_port.mac_bridge_port_num +
                        self._tp_table_id
                ),
                # Bridge Entity ID
                port_num=0xff,  # Port ID - unique number within the bridge
                tp_type=3,  # TP Type (IEEE 802.1p mapper service)
                tp_pointer=(
                        self._ieee_mapper_service_profile_entity_id +
                        self._uni_port.mac_bridge_port_num +
                        self._tp_table_id
                )
                # TP ID, 8021p mapper ID
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

        return None
