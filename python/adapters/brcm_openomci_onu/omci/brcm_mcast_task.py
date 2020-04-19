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
from pyvoltha.adapters.extensions.omci.omci_entities import OntG, PriorityQueueG, Tcont
from pyvoltha.adapters.extensions.omci.omci_me import MacBridgePortConfigurationDataFrame, AccessControlRow0, \
    MulticastOperationsProfileFrame, MulticastSubscriberConfigInfoFrame, VlanTaggingFilterDataFrame, \
    ExtendedVlanTaggingOperationConfigurationDataFrame, VlanTaggingOperation
from twisted.internet import reactor
from pyvoltha.adapters.extensions.omci.omci_defs import ReasonCodes, EntityOperations
from pyvoltha.adapters.extensions.omci.tasks.task import Task
from twisted.internet.defer import inlineCallbacks, failure


RC = ReasonCodes
OP = EntityOperations
IGMP_VERSION = 2
RESERVED_VLAN = 4095


class BrcmMcastTask(Task):
    task_priority = 150
    name = "Broadcom Multicast Task"

    def __init__(self, omci_agent, handler, device_id, uni_id, tp_id, vlan_id, dynamic_acl, static_acl, multicast_gem_id ):

        self.log = structlog.get_logger ( device_id=device_id )
        super ( BrcmMcastTask , self ).__init__ ( BrcmMcastTask.name ,
                                                  omci_agent ,
                                                  device_id,
                                                  self.task_priority)
        self._device = omci_agent.get_device ( device_id )
        self._results = None
        self._local_deferred = None
        self._uni_port = handler.uni_ports[uni_id]
        assert self._uni_port.uni_id == uni_id
        #mcast entities IDs
        self._mcast_operation_profile_id = handler.pon_port.mac_bridge_port_ani_entity_id + tp_id
        self._mcast_sub_conf_info_id = self._mcast_operation_profile_id + tp_id
        self._mac_bridge_service_profile_entity_id = handler.mac_bridge_service_profile_entity_id
        self._mac_bridge_configuration_data_id =self._mac_bridge_service_profile_entity_id +\
                                                self._uni_port.mac_bridge_port_num
        self._ieee_mapper_service_profile_entity_id = \
            handler.pon_port.ieee_mapper_service_profile_entity_id
        self._gal_enet_profile_entity_id = \
            handler.gal_enet_profile_entity_id
        # using for multicast operations profile and multicast subscriber config info
        self._set_igmp_function = 0
        self._set_immediate_leave = False
        self._set_vlan_id = vlan_id
        # dynamic_access_control_list , static_access_control_list and multicast_gem_id
        self._dynamic_acl= dynamic_acl
        self._static_acl= static_acl
        self._mcast_gem_id= multicast_gem_id
        self._uni_port = handler.uni_ports[uni_id]
        assert self._uni_port.uni_id == uni_id
        # gem_port list
        self._gem_ports = []
        for gem_port in list(handler.pon_port.gem_ports.values()):
            if gem_port.uni_id is not None and gem_port.uni_id != self._uni_port.uni_id: continue
            self._gem_ports.append(gem_port)

        self._tcont_list=[]
        for gem_port in self._gem_ports:
            self._tcont_list.append(gem_port.tcont)
        self.tcont_me_to_queue_map = dict()
        self.uni_port_to_queue_map = dict()

        self._set_me_type = 0
        self._set_interworking_option = 0
        self._store_tcont_list()

    def cancel_deferred(self):
        super ( BrcmMcastTask , self ).cancel_deferred ()
        d , self._local_deferred = self._local_deferred , None
        try:
            if d is not None and not d.called:
                d.cancel ()
        except:
            pass

    def start(self):
        super ( BrcmMcastTask , self ).start ()
        self._local_deferred = reactor.callLater ( 0 , self.perform_multicast )

    @inlineCallbacks
    def perform_multicast(self):
        self.log.debug('performing-multicast', device_id=self._device._device_id, uni_id=self._uni_port.uni_id,
                        vlan_id=self._set_vlan_id, multicast_gem_id=self._mcast_gem_id)

        try:
            # create gem port and use tcont
            yield self._create_gem_ports()

            # create multicast operation profile
            yield self._create_mcast_operation_profile()

            # set multicast operation profile
            yield self._set_mcast_operation_profile()

            # create multicast subscriber config data
            yield self._create_mcast_subscriber_conf_info()

            # create mac bridge port configuration data
            yield self._create_mac_bridge_configuration_data()

            # create vlan filtering entity
            yield self._create_vlan_tagging_filter_data()

            # set vlan filtering entity
            yield self._set_vlan_tagging_filter_data()

            self.deferred.callback ( self )
        except Exception as e:
            self.log.exception ( 'multicast exception' , e=e )
            self.deferred.errback ( failure.Failure ( e ) )

    @inlineCallbacks
    def _create_gem_ports(self):
        omci_cc = self._device.omci_cc
        try:
            tcont = None
            self.log.debug("tcont-list", _tcont_list=self._tcont_list)
            for gem_tcont in self._tcont_list:
                if gem_tcont.entity_id is not None:
                    tcont=gem_tcont
            for gem_port in self._gem_ports:
                gem_port_tcont= gem_port.tcont
                if gem_port_tcont is not None and gem_port_tcont.entity_id is not None:
                    tcont = gem_port.tcont
                ul_prior_q_entity_id = \
                    self.tcont_me_to_queue_map[tcont.entity_id][gem_port.priority_q]
                self.log.debug("ul_prior_q_entity_id", ul_priority=ul_prior_q_entity_id)

                dl_prior_q_entity_id = \
                    self.uni_port_to_queue_map[self._uni_port.entity_id][gem_port.priority_q]
                if ul_prior_q_entity_id is not None and dl_prior_q_entity_id is not None:
                    if gem_port.direction == 'downstream' and gem_port.gem_id == self._mcast_gem_id:
                        results = yield gem_port.add_to_hardware ( omci=omci_cc ,
                                                                   tcont_entity_id=tcont.entity_id ,
                                                                   ieee_mapper_service_profile_entity_id=self._ieee_mapper_service_profile_entity_id +
                                                                                                         self._uni_port.mac_bridge_port_num ,
                                                                   gal_enet_profile_entity_id=self._gal_enet_profile_entity_id ,
                                                                   ul_prior_q_entity_id=ul_prior_q_entity_id ,
                                                                   dl_prior_q_entity_id=dl_prior_q_entity_id)
                        self.check_status_and_state ( results , 'assign-gem-port' )
                        pass
        except Exception as e:
            self.log.debug("failed-create-gem-ports-method", e=e)

    def _store_tcont_list(self):
        pq_to_related_port = dict()
        onu_g = self._device.query_mib ( OntG.class_id )
        traffic_mgmt_opt = \
            onu_g.get ( 'attributes' , {} ).get ( 'traffic_management_options' , 0 )
        self.log.debug ( "traffic-mgmt-option" , traffic_mgmt_opt=traffic_mgmt_opt )

        prior_q = self._device.query_mib ( PriorityQueueG.class_id )
        for k , v in prior_q.items():
            self.log.debug("prior-q", k=k, v=v)
            try:
                _ = iter(v)
            except TypeError:
                continue

            if 'instance_id' in v:
                related_port = v['attributes']['related_port']
                pq_to_related_port[k] = related_port

                if v['instance_id'] & 0b1000000000000000:
                    tcont_me = (related_port & 0xffff0000) >> 16
                    if tcont_me not in self.tcont_me_to_queue_map:
                        self.log.debug ( "prior-q-related-port-and-tcont-me" ,
                                         related_port=related_port ,
                                         tcont_me=tcont_me )
                        self.tcont_me_to_queue_map[tcont_me] = list ()

                    self.tcont_me_to_queue_map[tcont_me].append ( k )
                else:
                    uni_port = (related_port & 0xffff0000) >> 16
                    if uni_port == self._uni_port.entity_id:
                        if uni_port not in self.uni_port_to_queue_map:
                            self.log.debug ( "prior-q-related-port-and-uni-port-me" ,
                                             related_port=related_port ,
                                             uni_port_me=uni_port )
                            self.uni_port_to_queue_map[uni_port] = list ()

                        self.uni_port_to_queue_map[uni_port].append ( k )


    @inlineCallbacks
    def _create_mac_bridge_configuration_data(self):
        self.log.debug ( 'starting-create-mac-bridge-conf-data' )
        attributes = dict ( bridge_id_pointer=self._mac_bridge_configuration_data_id,
                            port_num=0xf0,
                            tp_type=6,
                            tp_pointer=self._mcast_gem_id)
        msg = MacBridgePortConfigurationDataFrame ( entity_id=self._mac_bridge_configuration_data_id, attributes=attributes )
        yield self._send_msg ( msg , 'create' , 'create-mac-bridge-port-conf-data' )


    @inlineCallbacks
    def _create_mcast_operation_profile(self):
        self.log.debug ( 'starting-create-mcast-operation-profile' )
        attributes = dict ( igmp_version=IGMP_VERSION , igmp_function=self._set_igmp_function,
                            immediate_leave=self._set_immediate_leave, robustness=2 )
        msg = MulticastOperationsProfileFrame ( entity_id=self._mcast_operation_profile_id,
                                                querier_ip_address=0,
                                                query_interval=125,
                                                query_max_response=100,
                                                last_member_query_interval=10,
                                                unauthorized_join_request_behavior=False,
                                                attributes=attributes)
        yield self._send_msg ( msg , 'create' , 'create-multicast-operation-profile')

    @inlineCallbacks
    def _set_mcast_operation_profile(self):
        self.log.debug('starting-set-mcast-operation-profile')
        dynamic_access_control_list_table= AccessControlRow0 (
            set_ctrl=1 ,
            row_part_id=0 ,
            test=0 ,
            row_key=0 ,
            gem_port_id=self._mcast_gem_id,
            vlan_id=self._set_vlan_id,
            src_ip="0.0.0.0",
            dst_ip_start=self._dynamic_acl[0],
            dst_ip_end=self._dynamic_acl[1],
            ipm_group_bw=0

        )
        msg = MulticastOperationsProfileFrame(entity_id=self._mcast_operation_profile_id,
                                              dynamic_access_control_list_table=dynamic_access_control_list_table
                                             )
        yield self._send_msg(msg, 'set', 'set-multicast-operations-profile')

    @inlineCallbacks
    def _create_mcast_subscriber_conf_info(self):
        self.log.debug ( 'creating-multicast-subscriber-config-info' )
        attributes = dict ( me_type=self._set_me_type,
                            multicast_operations_profile_pointer=self._mcast_operation_profile_id )
        msg = MulticastSubscriberConfigInfoFrame ( entity_id=self._uni_port.entity_id, attributes=attributes )
        yield self._send_msg ( msg , 'create' , 'create-multicast-subscriber-config-info' )


    @inlineCallbacks
    def _create_vlan_tagging_filter_data(self):
        self.log.debug('creating-vlan-tagging-filter-data')
        forward_operation = 0x10  # VID investigation
        # When the PUSH VLAN is RESERVED_VLAN (4095), let ONU be transparent
        if self._set_vlan_id == RESERVED_VLAN:
            forward_operation = 0x00  # no investigation, ONU transparent

        # Create bridge ani side vlan filter
        msg = VlanTaggingFilterDataFrame(
            self._mac_bridge_configuration_data_id,  # Entity ID
            vlan_tcis=[self._set_vlan_id],  # VLAN IDs
            forward_operation=forward_operation
        )

        yield self._send_msg(msg, 'create', 'flow-create-vlan-tagging-filter-data')

    @inlineCallbacks
    def _set_vlan_tagging_filter_data(self):
        self.log.debug('setting-vlan-tagging-filter-data')
        forward_operation = 0x10  # VID investigation
        # When the PUSH VLAN is RESERVED_VLAN (4095), let ONU be transparent
        if self._set_vlan_id == RESERVED_VLAN:
            forward_operation = 0x00  # no investigation, ONU transparent

        # Create bridge ani side vlan filter
        msg = VlanTaggingFilterDataFrame(
            self._mac_bridge_configuration_data_id,  # Entity ID
            vlan_tcis=[self._set_vlan_id],  # VLAN IDs
            forward_operation=forward_operation
        )

        yield self._send_msg(msg, 'set', 'flow-set-vlan-tagging-filter-data')


    @inlineCallbacks
    def _send_msg(self , msg , operation , log_comment):
        if operation == 'create':
            frame = msg.create ()
        elif operation == 'set':
            frame = msg.set ()
        else:
            frame = msg.delete ()
        self.log.debug ( 'openomci-msg' , omci_msg=msg )
        self.strobe_watchdog ()
        results = yield self._device.omci_cc.send ( frame )
        self.check_status_and_state ( results , log_comment )

    def check_status_and_state(self , results , operation=''):
        """
        Check the results of an OMCI response.  An exception is thrown
        if the task was cancelled or an error was detected.

        :param results: (OmciFrame) OMCI Response frame
        :param operation: (str) what operation was being performed
        :return: True if successful, False if the entity existed (already created)
        """

        omci_msg = results.fields['omci_message'].fields
        status = omci_msg['success_code']
        error_mask = omci_msg.get ( 'parameter_error_attributes_mask' , 'n/a' )
        failed_mask = omci_msg.get ( 'failed_attributes_mask' , 'n/a' )
        unsupported_mask = omci_msg.get ( 'unsupported_attributes_mask' , 'n/a' )

        self.log.debug ( "OMCI Result: %s" , operation , omci_msg=omci_msg ,
                         status=status , error_mask=error_mask ,
                         failed_mask=failed_mask , unsupported_mask=unsupported_mask )

        if status == RC.Success:
            self.strobe_watchdog ()
            return True

        elif status == RC.InstanceExists:
            return False
