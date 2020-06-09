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
from pyvoltha.adapters.extensions.omci.omci_entities import VlanTaggingFilterData
from pyvoltha.adapters.extensions.omci.omci_me import \
    VlanTaggingOperation, VlanTaggingFilterDataFrame, ExtendedVlanTaggingOperationConfigurationDataFrame, \
    ExtendedVlanTaggingOperationConfigurationData
from uni_port import UniType
from uni_port import RESERVED_TRANSPARENT_VLAN
from pon_port import DEFAULT_TPID

RC = ReasonCodes
OP = EntityOperations


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
        :param add_tag: (bool) Flag to identify VLAN Tagging or Untagging
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

        self._onu_handler = handler
        self._device = omci_agent.get_device(handler.device_id)
        self._uni_port = uni_port
        self._set_vlan_id = set_vlan_id

        # If not setting any pbit, copy the pbit from the received vlan tag
        if set_vlan_pcp is None:
            self._set_vlan_pcp = 8  # Copy from the inner priority of the received frame
        else:
            self._set_vlan_pcp = set_vlan_pcp

        # If not matching on any vlan do not filter for vlan value.  effectively match on untagged
        if match_vlan is None:
            self._match_vlan = RESERVED_TRANSPARENT_VLAN  # Do not filter on the VID
        else:
            self._match_vlan = match_vlan

        self._vlan_pcp = 8  # Do not filter on priority

        # If the tag is not transparent then need to remove it.
        self._treatment_tags_to_remove = 1
        if self._match_vlan == RESERVED_TRANSPARENT_VLAN:  # Do not filter on the VID
            self._vlan_pcp = 15  # This entry is a no-tag rule; ignore all other VLAN tag filter fields
            self._treatment_tags_to_remove = 0

        # If matching on untagged and trying to copy pbit from non-existent received vlan, set pbit to zero
        if self._match_vlan == RESERVED_TRANSPARENT_VLAN and self._set_vlan_pcp == 8:
            self._set_vlan_pcp = 0

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
        self.log.debug('vlan-filter-tagging-task', uni_port=self._uni_port, set_vlan_id=self._set_vlan_id,
                       set_vlan_pcp=self._set_vlan_pcp, match_vlan=self._match_vlan, tp_id=self._tp_id,
                       add_tag=self._add_tag)
        try:
            if self._add_tag:
                if not self._onu_handler.args.accept_incremental_evto_update:
                    yield self._bulk_update_evto_and_vlan_tag_filter()
                else:
                    yield self._incremental_update_evto_and_vlan_tag_filter()
            else:  # addTag = False
                if self._onu_handler.args.accept_incremental_evto_update:
                    self.log.info('removing-vlan-tagging')
                    yield self._delete_service_flow()
                    yield self._delete_vlan_filter_entity()
                else:
                    # Will be reset anyway on new vlan tagging operation
                    self.log.info("not-removing-vlan-tagging")
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

    @inlineCallbacks
    def _delete_service_flow(self):
        extended_vlan_tagging_entity_id = self._mac_bridge_service_profile_entity_id + \
                                          self._uni_port.mac_bridge_port_num

        # See G.988 regarding evto row deletes for "default" flows:
        #
        # As an exception to the rule on ordered processing, these default rules are always
        # considered as a last resort for frames that do not match any other rule. Best
        # practice dictates that these entries not be deleted by the OLT; however, they
        # can be modified to produce the desired default behaviour.
        #
        # 15, 4096, x, 15, 4096, x, 0, (0, 15, x, x, 15, x, x) – no tags
        # 15, 4096, x, 14, 4096, x, 0, (0, 15, x, x, 15, x, x) – 1 tag
        # 14, 4096, x, 14, 4096, x, 0, (0, 15, x, x, 15, x, x) – 2 tags

        # outer_prio is 15, outer vid is 4096  inner prio is 15, inner vid is 4096
        # its a default rule... dont delete it.
        if self._match_vlan == RESERVED_TRANSPARENT_VLAN and self._vlan_pcp == 15:
            self.log.warn("should-not-delete-onu-builtin-no-tag-flow")
            return

        # outer_prio is 15, outer vid is 4096  inner prio is 14, inner vid is 4096
        # its a default rule... dont delete it.
        if self._match_vlan == RESERVED_TRANSPARENT_VLAN and self._vlan_pcp == 14:
            self.log.warn("should-not-delete-onu-builtin-single-tag-flow")
            return

        entry = VlanTaggingOperation(
            filter_outer_priority=15,
            filter_outer_vid=4096,
            filter_outer_tpid_de=0,

            filter_inner_priority=self._vlan_pcp,
            filter_inner_vid=self._match_vlan,
            filter_inner_tpid_de=0,
            filter_ether_type=0
        )
        # delete this entry using the filter rules as the key.
        # this function automatically fills 0xFF in the last 8 treatment bytes
        entry = entry.delete()
        attributes = dict(received_frame_vlan_tagging_operation_table=entry)

        msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
            extended_vlan_tagging_entity_id,  # Bridge Entity ID
            attributes=attributes  # See above
        )
        frame = msg.set()
        self.log.debug('openomci-msg', omci_msg=msg)
        results = yield self._device.omci_cc.send(frame)
        self.check_status_and_state(results, 'delete-service-flow')

    @inlineCallbacks
    def _delete_vlan_filter_entity(self):
        vlan_tagging_entity_id = int(self._mac_bridge_port_ani_entity_id + self._uni_port.entity_id
                                     + self._tp_id)
        self.log.debug("Vlan tagging filter data frame will be deleted.",
                       expected_me_id=vlan_tagging_entity_id)
        msg = VlanTaggingFilterDataFrame(vlan_tagging_entity_id)
        frame = msg.delete()
        self.log.debug('openomci-msg', omci_msg=msg)
        results = yield self._device.omci_cc.send(frame)
        self.check_status_and_state(results, 'flow-delete-vlan-tagging-filter-data')

    @inlineCallbacks
    def _create_vlan_filter_entity(self, vlan_tagging_entity_id):

        self.log.debug("Vlan tagging filter data frame will be created.",
                       expected_me_id=vlan_tagging_entity_id)
        vlan_tagging_me = self._device.query_mib(VlanTaggingFilterData.class_id,
                                                 instance_id=int(vlan_tagging_entity_id))
        if len(vlan_tagging_me) == 0:
            forward_operation = 0x10  # VID investigation
            self.log.debug("vlan id isn't reserved")
            self.log.debug("forward_operation", forward_operation=forward_operation)
            self.log.debug("set_vlan_id", vlan_id=self._set_vlan_id)
            # When the PUSH VLAN is RESERVED_VLAN (4095), let ONU be transparent
            if self._set_vlan_id == RESERVED_TRANSPARENT_VLAN:
                forward_operation = 0x00  # no investigation, ONU transparent

            # Create bridge ani side vlan filter
            msg = VlanTaggingFilterDataFrame(
                int(vlan_tagging_entity_id),  # Entity ID
                vlan_tcis=[self._set_vlan_id],  # VLAN IDs
                forward_operation=forward_operation
            )

            self.log.debug("created vlan tagging data frame msg")
            frame = msg.create()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield self._device.omci_cc.send(frame)
            self.check_status_and_state(results, 'create-vlan-tagging-filter-data')

    @inlineCallbacks
    def _reset_evto_and_vlan_tag_filter(self):
        self.log.info("resetting-evto-and-vlan-tag-filter")
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
            association_type=association_type,  # Assoc Type, PPTP/VEIP Ethernet UNI
            associated_me_pointer=self._uni_port.entity_id,  # Assoc ME, PPTP/VEIP Entity Id

            # See VOL-1311 - Need to set table during create to avoid exception
            # trying to read back table during post-create-read-missing-attributes
            # But, because this is a R/W attribute. Some ONU may not accept the
            # value during create. It is repeated again in a set below.
            input_tpid=self._input_tpid,  # input TPID
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

    @inlineCallbacks
    def _incremental_update_evto_and_vlan_tag_filter(self):
        self.log.info("incremental-update-evto-and-vlan-tag-filter")
        vlan_tagging_entity_id = int(self._mac_bridge_port_ani_entity_id + self._uni_port.entity_id
                                     + self._tp_id)
        extended_vlan_tagging_entity_id = self._mac_bridge_service_profile_entity_id + \
                                          self._uni_port.mac_bridge_port_num
        ################################################################################
        # VLAN Tagging Filter config
        #
        #  EntityID will be referenced by:
        #            - Nothing
        #  References:
        #            - MacBridgePortConfigurationData for the ANI/PON side
        #

        # TODO: check if its in our local mib first before blindly deleting
        if self._tp_id != 0:
            yield self._create_vlan_filter_entity(vlan_tagging_entity_id)

        self.log.info('setting-vlan-tagging')

        # Onu-Transparent
        if self._set_vlan_id == RESERVED_TRANSPARENT_VLAN:
            # Transparently send any single tagged packet.
            # As the onu is to be transparent, no need to create VlanTaggingFilterData ME.
            # Any other specific rules will take priority over this, so not setting any other vlan specific rules
            entry = VlanTaggingOperation(
                filter_outer_priority=15,  # not an outer tag rule, ignore all other outers
                filter_outer_vid=4096,  # ignore
                filter_outer_tpid_de=0,  # ignore
                filter_inner_priority=14,  # default single tagged rule
                filter_inner_vid=4096,  # do not match on vlan value
                filter_inner_tpid_de=0,  # do not match on tpid
                filter_ether_type=0,  # do not filter on untagged ethertype
                treatment_tags_to_remove=0,  # do not remove any tags
                treatment_outer_priority=15,  # do not add outer tag
                treatment_outer_vid=0,  # ignore
                treatment_outer_tpid_de=0,  # ignore
                treatment_inner_priority=15,  # do not add inner tag
                treatment_inner_vid=0,  # ignore
                treatment_inner_tpid_de=4  # set TPID 0x8100
            )

            attributes = dict(received_frame_vlan_tagging_operation_table=entry)
            msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
                extended_vlan_tagging_entity_id,  # Bridge Entity ID
                attributes=attributes
            )

            frame = msg.set()
            self.log.debug('openomci-msg', omci_msg=msg)
            self.strobe_watchdog()
            results = yield self._device.omci_cc.send(frame)
            self.check_status_and_state(results, 'set-evto-table-transparent-vlan')
        else:
            # Update uni side extended vlan filter
            # filter for any set vlan - even its match tag is TRANSPARENT.
            # For TRANSPARENT match_vlan tag case we modified vlan_pcp and treatment_tags_to_remove values during init.
            entry = VlanTaggingOperation(
                filter_outer_priority=15,  # This entry is not a double-tag rule
                filter_outer_vid=4096,  # Do not filter on the outer VID value
                filter_outer_tpid_de=0,  # Do not filter on the outer TPID field

                filter_inner_priority=self._vlan_pcp,  # Filter on inner vlan
                filter_inner_vid=self._match_vlan,  # Look for match vlan
                filter_inner_tpid_de=0,  # Do not filter on inner TPID field
                filter_ether_type=0,  # Do not filter on EtherType

                treatment_tags_to_remove=self._treatment_tags_to_remove,
                treatment_outer_priority=15,
                treatment_outer_vid=0,
                treatment_outer_tpid_de=0,

                treatment_inner_priority=self._set_vlan_pcp,  # Add an inner priority
                treatment_inner_vid=self._set_vlan_id,  # use this value as the VID in the inner VLAN tag
                treatment_inner_tpid_de=4,  # set TPID
            )

            attributes = dict(received_frame_vlan_tagging_operation_table=entry)
            msg = ExtendedVlanTaggingOperationConfigurationDataFrame(
                extended_vlan_tagging_entity_id,  # Bridge Entity ID
                attributes=attributes  # See above
            )
            frame = msg.set()
            self.log.debug('openomci-msg', omci_msg=msg)
            self.strobe_watchdog()
            results = yield self._device.omci_cc.send(frame)
            self.check_status_and_state(results, 'set-evto-table')

    @inlineCallbacks
    def _bulk_update_evto_and_vlan_tag_filter(self):
        self.log.info("bulk-update-evto-and-vlan-tag-filter")
        # First reset any existing config EVTO and vlan tag filter on the ONU
        yield self._reset_evto_and_vlan_tag_filter()

        vlan_tagging_entity_id = int(self._mac_bridge_port_ani_entity_id + self._uni_port.entity_id
                                     + self._tp_id)
        extended_vlan_tagging_entity_id = self._mac_bridge_service_profile_entity_id + \
                                          self._uni_port.mac_bridge_port_num

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
                extended_vlan_tagging_entity_id,  # Bridge Entity ID
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
                vlan_tagging_entity_id,
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
                extended_vlan_tagging_entity_id,  # Bridge Entity ID
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
                extended_vlan_tagging_entity_id,  # Bridge Entity ID
                attributes=attributes  # See above
            )
            frame = msg.set()
            self.log.debug('openomci-msg', omci_msg=msg)
            self.strobe_watchdog()
            results = yield self._device.omci_cc.send(frame)
            self.check_status_and_state(results, 'set-evto-table-zero-tagged')
