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
from twisted.internet.defer import inlineCallbacks, failure, returnValue
from pyvoltha.adapters.extensions.omci.omci_defs import ReasonCodes, EntityOperations
from pyvoltha.adapters.extensions.omci.omci_me import \
    VlanTaggingOperation, VlanTaggingFilterDataFrame, ExtendedVlanTaggingOperationConfigurationDataFrame
from uni_port import UniType
from pon_port import DEFAULT_TPID

RC = ReasonCodes
OP = EntityOperations
RESERVED_TRANSPARENT_VLAN = 4095


class BrcmVlanFilterException(Exception):
    pass


class BrcmVlanFilterTask(Task):
    """
    Apply Vlan Tagging Filter Data and Extended VLAN Tagging Operation Configuration on an ANI and UNI
    """
    task_priority = 200
    name = "Broadcom VLAN Filter/Tagging Task"

    def __init__(self, omci_agent, handler, uni_port, set_vlan_id,
                 match_vlan=0, set_vlan_pcp=8, add_tag=True,
                 priority=task_priority, tp_id=0):
        """
        Class initialization

        :param omci_agent: (OmciAdapterAgent) OMCI Adapter agent
        :param handler: (BrcmOpenomciOnuHandler) ONU Device Handler Instance
        :param uni_port: (UniPort) Object instance representing the uni port and its settings
        :param set_vlan_id: (int) VLAN to filter for and set
        :param tp_id: (int) TP ID for the flow
        :param priority: (int) OpenOMCI Task priority (0..255) 255 is the highest
        """
        super(BrcmVlanFilterTask, self).__init__(BrcmVlanFilterTask.name,
                                                 omci_agent,
                                                 handler.device_id,
                                                 priority=priority,
                                                 exclusive=True)

        self.log = structlog.get_logger(device_id=handler.device_id,
                                        name=BrcmVlanFilterTask.name,
                                        task_id=self._task_id,
                                        entity_id=uni_port.entity_id,
                                        uni_id=uni_port.uni_id,
                                        uni_port=uni_port.port_number,
                                        set_vlan_id=set_vlan_id)

        self._device = omci_agent.get_device(handler.device_id)
        self._uni_port = uni_port
        self._set_vlan_id = set_vlan_id
        self._set_vlan_pcp = set_vlan_pcp
        self._match_vlan = match_vlan
        self._add_tag = add_tag
        self._tp_id = tp_id
        self._results = None
        self._local_deferred = None
        self._config = self._device.configuration

        self._input_tpid = DEFAULT_TPID
        self._output_tpid = DEFAULT_TPID

        self._mac_bridge_service_profile_entity_id = \
            handler.mac_bridge_service_profile_entity_id
        self._ieee_mapper_service_profile_entity_id = \
            handler.pon_port.ieee_mapper_service_profile_entity_id
        self._mac_bridge_port_ani_entity_id = \
            handler.pon_port.mac_bridge_port_ani_entity_id
        self._gal_enet_profile_entity_id = \
            handler.gal_enet_profile_entity_id

    def cancel_deferred(self):
        super(BrcmVlanFilterTask, self).cancel_deferred()

        d, self._local_deferred = self._local_deferred, None
        try:
            if d is not None and not d.called:
                d.cancel()
        except:
            pass

    def start(self):
        """
        Start Vlan Tagging Task
        """
        super(BrcmVlanFilterTask, self).start()
        self._local_deferred = reactor.callLater(0, self.perform_vlan_tagging)

    @inlineCallbacks
    def perform_vlan_tagging(self):
        """
        Perform the vlan tagging
        """
        self.log.debug('vlan-filter-tagging-task', device_id=self._device._device_id, uni_port=self._uni_port, set_vlan_id=self._set_vlan_id, \
            set_vlan_pcp=self._set_vlan_pcp, match_vlan=self._match_vlan, tp_id=self._tp_id, add_tag=self._add_tag)

        try:
            if self._add_tag:
                ################################################################################
                # VLAN Tagging Filter config
                #
                #  EntityID will be referenced by:
                #            - Nothing
                #  References:
                #            - MacBridgePortConfigurationData for the ANI/PON side
                #


                # Delete bridge ani side vlan filter
                eid = self._mac_bridge_port_ani_entity_id + self._uni_port.entity_id + self._tp_id  # Entity ID
                msg = VlanTaggingFilterDataFrame(eid)
                frame = msg.delete()
                self.log.debug('openomci-msg', omci_msg=msg)
                self.strobe_watchdog()
                results = yield self._device.omci_cc.send(frame)
                self.check_status_and_state(results, 'flow-delete-vlan-tagging-filter-data')

                ################################################################################
                # Create Extended VLAN Tagging Operation config (UNI-side)
                #
                #  EntityID relates to the VLAN TCIS later used int vlan filter task.  This only
                #  sets up the inital MIB entry as it relates to port config, it does not set vlan
                #  that is saved for the vlan filter task
                #
                #  References:
                #            - PPTP Ethernet or VEIP UNI
                #


                # Delete uni side evto
                msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
                    self._mac_bridge_service_profile_entity_id + self._uni_port.mac_bridge_port_num,
                )
                frame = msg.delete()
                self.log.debug('openomci-msg', omci_msg=msg)
                results = yield self._device.omci_cc.send(frame)
                self.check_status_and_state(results, 'delete-extended-vlan-tagging-operation-configuration-data')


                # Re-Create uni side evto
                # default to PPTP
                association_type = 2
                if self._uni_port.type.value == UniType.VEIP.value:
                    association_type = 10
                elif self._uni_port.type.value == UniType.PPTP.value:
                    association_type = 2

                attributes = dict(
                    association_type=association_type,             # Assoc Type, PPTP/VEIP Ethernet UNI
                    associated_me_pointer=self._uni_port.entity_id,      # Assoc ME, PPTP/VEIP Entity Id

                    # See VOL-1311 - Need to set table during create to avoid exception
                    # trying to read back table during post-create-read-missing-attributes
                    # But, because this is a R/W attribute. Some ONU may not accept the
                    # value during create. It is repeated again in a set below.
                    input_tpid=self._input_tpid,    # input TPID
                    output_tpid=self._output_tpid,  # output TPID
                )

                msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
                    self._mac_bridge_service_profile_entity_id + self._uni_port.mac_bridge_port_num,  # Bridge Entity ID
                    attributes=attributes
                )

                frame = msg.create()
                self.log.debug('openomci-msg', omci_msg=msg)
                results = yield self._device.omci_cc.send(frame)
                self.check_status_and_state(results, 'create-extended-vlan-tagging-operation-configuration-data')

                attributes = dict(
                    # Specifies the TPIDs in use and that operations in the downstream direction are
                    # inverse to the operations in the upstream direction
                    input_tpid=self._input_tpid,    # input TPID
                    output_tpid=self._output_tpid,  # output TPID
                    downstream_mode=0,              # inverse of upstream
                )

                msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
                    self._mac_bridge_service_profile_entity_id + self._uni_port.mac_bridge_port_num,  # Bridge Entity ID
                    attributes=attributes
                )

                frame = msg.set()
                self.log.debug('openomci-msg', omci_msg=msg)
                self.strobe_watchdog()
                results = yield self._device.omci_cc.send(frame)
                self.check_status_and_state(results, 'set-extended-vlan-tagging-operation-configuration-data')

                # Onu-Transparent
                if self._set_vlan_id == RESERVED_TRANSPARENT_VLAN:
                    # Transparently send any single tagged packet.
                    # As the onu is to be transparent, no need to create VlanTaggingFilterData ME.
                    # Any other specific rules will take priority over this, so not setting any other vlan specific rules
                    attributes = dict(
                        received_frame_vlan_tagging_operation_table=
                        VlanTaggingOperation(
                            filter_outer_priority=15,
                            filter_outer_vid=4096,
                            filter_outer_tpid_de=0,
                            filter_inner_priority=14,
                            filter_inner_vid=4096,
                            filter_inner_tpid_de=0,
                            filter_ether_type=0,
                            treatment_tags_to_remove=0,
                            treatment_outer_priority=15,
                            treatment_outer_vid=0,
                            treatment_outer_tpid_de=0,
                            treatment_inner_priority=15,
                            treatment_inner_vid=0,
                            treatment_inner_tpid_de=4
                        )
                    )

                    msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
                        self._mac_bridge_service_profile_entity_id + self._uni_port.mac_bridge_port_num,  # Bridge Entity ID
                        attributes=attributes
                    )

                    frame = msg.set()
                    self.log.debug('openomci-msg', omci_msg=msg)
                    self.strobe_watchdog()
                    results = yield self._device.omci_cc.send(frame)
                    self.check_status_and_state(results, 'set-evto-table-transparent-vlan')

                else:
                    # Re-Create bridge ani side vlan filter
                    forward_operation = 0x10  # VID investigation

                    msg = VlanTaggingFilterDataFrame(
                        eid,
                        vlan_tcis=[self._set_vlan_id],  # VLAN IDs
                        forward_operation=forward_operation
                    )
                    frame = msg.create()
                    self.log.debug('openomci-msg', omci_msg=msg)
                    self.strobe_watchdog()
                    results = yield self._device.omci_cc.send(frame)
                    self.check_status_and_state(results, 'flow-create-vlan-tagging-filter-data')
                    # Update uni side extended vlan filter
                    # filter for untagged
                    attributes = dict(
                        received_frame_vlan_tagging_operation_table=
                        VlanTaggingOperation(
                            filter_outer_priority=15,
                            filter_outer_vid=4096,
                            filter_outer_tpid_de=0,
                            filter_inner_priority=15,
                            filter_inner_vid=4096,
                            filter_inner_tpid_de=0,
                            filter_ether_type=0,
                            treatment_tags_to_remove=0,
                            treatment_outer_priority=15,
                            treatment_outer_vid=0,
                            treatment_outer_tpid_de=0,
                            treatment_inner_priority=0,
                            treatment_inner_vid=self._set_vlan_id,
                            treatment_inner_tpid_de=4
                        )
                    )

                    msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
                        self._mac_bridge_service_profile_entity_id + self._uni_port.mac_bridge_port_num,  # Bridge Entity ID
                        attributes=attributes
                    )

                    frame = msg.set()
                    self.log.debug('openomci-msg', omci_msg=msg)
                    self.strobe_watchdog()
                    results = yield self._device.omci_cc.send(frame)
                    self.check_status_and_state(results, 'set-evto-table-untagged')

                    # Update uni side extended vlan filter
                    # filter for vlan 0
                    attributes = dict(
                        received_frame_vlan_tagging_operation_table=
                        VlanTaggingOperation(
                            filter_outer_priority=15,  # This entry is not a double-tag rule
                            filter_outer_vid=4096,  # Do not filter on the outer VID value
                            filter_outer_tpid_de=0,  # Do not filter on the outer TPID field

                            filter_inner_priority=8,  # Filter on inner vlan
                            filter_inner_vid=0x0,  # Look for vlan 0
                            filter_inner_tpid_de=0,  # Do not filter on inner TPID field
                            filter_ether_type=0,  # Do not filter on EtherType

                            treatment_tags_to_remove=1,
                            treatment_outer_priority=15,
                            treatment_outer_vid=0,
                            treatment_outer_tpid_de=0,

                            treatment_inner_priority=8,  # Add an inner tag and insert this value as the priority
                            treatment_inner_vid=self._set_vlan_id,  # use this value as the VID in the inner VLAN tag
                            treatment_inner_tpid_de=4,  # set TPID
                        )
                    )
                    msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
                        self._mac_bridge_service_profile_entity_id + self._uni_port.mac_bridge_port_num,  # Bridge Entity ID
                        attributes=attributes  # See above
                    )
                    frame = msg.set()
                    self.log.debug('openomci-msg', omci_msg=msg)
                    self.strobe_watchdog()
                    results = yield self._device.omci_cc.send(frame)
                    self.check_status_and_state(results, 'set-evto-table-zero-tagged')

            else: #addTag = False
                #TODO: needs to be implemented - future task.
                self.log.debug('removing-vlan-filter-tag', uni_port=self._uni_port, match_vlan=self._match_vlan, )
            self.deferred.callback(self)

        except Exception as e:
            self.log.exception('setting-vlan-tagging', e=e)
            self.deferred.errback(failure.Failure(e))

    def check_status_and_state(self, results, operation=''):
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

        self.log.debug("OMCI Result", operation=operation, omci_msg=omci_msg,
                       status=status, error_mask=error_mask,
                       failed_mask=failed_mask, unsupported_mask=unsupported_mask)

        if status == RC.Success:
            self.strobe_watchdog()
            return True

        elif status == RC.InstanceExists:
            return False

