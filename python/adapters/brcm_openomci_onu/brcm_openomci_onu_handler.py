#
# Copyright 2017 the original author or authors.
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

"""
Broadcom OpenOMCI OLT/ONU adapter handler.
"""

from __future__ import absolute_import
import six
import arrow
import structlog
import json
import random

from collections import OrderedDict

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from heartbeat import HeartBeat
from pyvoltha.adapters.extensions.events.device_events.onu.onu_active_event import OnuActiveEvent
from pyvoltha.adapters.extensions.events.device_events.onu.onu_disabled_event import OnuDisabledEvent
from pyvoltha.adapters.extensions.events.device_events.onu.onu_deleted_event import OnuDeletedEvent
from pyvoltha.adapters.extensions.events.kpi.onu.onu_pm_metrics import OnuPmMetrics
from pyvoltha.adapters.extensions.events.kpi.onu.onu_omci_pm import OnuOmciPmMetrics
from pyvoltha.adapters.extensions.events.adapter_events import AdapterEvents

import pyvoltha.common.openflow.utils as fd
from pyvoltha.common.utils.registry import registry
from pyvoltha.adapters.common.frameio.frameio import hexify
from pyvoltha.common.utils.nethelpers import mac_str_to_tuple
from pyvoltha.adapters.common.kvstore.twisted_etcd_store import TwistedEtcdStore
from voltha_protos.logical_device_pb2 import LogicalPort
from voltha_protos.common_pb2 import OperStatus, ConnectStatus, AdminState
from voltha_protos.device_pb2 import Port
from voltha_protos.openflow_13_pb2 import OFPXMC_OPENFLOW_BASIC, ofp_port, OFPPS_LIVE, OFPPS_LINK_DOWN, \
    OFPPF_FIBER, OFPPF_1GB_FD
from voltha_protos.inter_container_pb2 import InterAdapterMessageType, \
    InterAdapterOmciMessage, PortCapability, InterAdapterTechProfileDownloadMessage, InterAdapterDeleteGemPortMessage, \
    InterAdapterDeleteTcontMessage
from voltha_protos.openolt_pb2 import OnuIndication
from pyvoltha.adapters.extensions.omci.onu_device_entry import OnuDeviceEvents, \
    OnuDeviceEntry, IN_SYNC_KEY
from omci.brcm_mib_download_task import BrcmMibDownloadTask
from omci.brcm_tp_setup_task import BrcmTpSetupTask
from omci.brcm_tp_delete_task import BrcmTpDeleteTask
from omci.brcm_uni_lock_task import BrcmUniLockTask
from omci.brcm_vlan_filter_task import BrcmVlanFilterTask
from onu_gem_port import OnuGemPort
from onu_tcont import OnuTCont
from pon_port import PonPort
from omci.brcm_mcast_task import BrcmMcastTask
from uni_port import UniPort, UniType
from uni_port import RESERVED_TRANSPARENT_VLAN
from pyvoltha.common.tech_profile.tech_profile import TechProfile
from pyvoltha.adapters.extensions.omci.tasks.omci_test_request import OmciTestRequest
from pyvoltha.adapters.extensions.omci.omci_entities import AniG, Tcont, MacBridgeServiceProfile
from pyvoltha.adapters.extensions.omci.omci_defs import EntityOperations, ReasonCodes
from voltha_protos.voltha_pb2 import TestResponse

OP = EntityOperations
RC = ReasonCodes

IS_MULTICAST = 'is_multicast'
GEM_PORT_ID = 'gemport_id'
_STARTUP_RETRY_WAIT = 10
_PATH_SEPERATOR = "/"


class BrcmOpenomciOnuHandler(object):

    def __init__(self, adapter, device_id):
        self.log = structlog.get_logger(device_id=device_id)
        self.log.debug('starting-handler')
        self.adapter = adapter
        self.core_proxy = adapter.core_proxy
        self.adapter_proxy = adapter.adapter_proxy
        self.parent_id = None
        self.device_id = device_id
        self.proxy_address = None
        self._enabled = False
        self.events = None
        self._pm_metrics = None
        self._pm_metrics_started = False
        self._test_request = None
        self._test_request_started = False
        self._tp = dict()  # tp_id -> technology profile definition in KV Store.
        self._reconciling = False

        # Persisted onu configuration needed in case of reconciliation.
        self._onu_persisted_state = {
            'onu_id': None,
            'intf_id': None,
            'serial_number': None,
            'admin_state': None,
            'oper_state': None,
            'uni_config': list()
        }

        self._unis = dict()  # Port # -> UniPort

        self._pon = None
        self._pon_port_number = 100
        self.logical_device_id = None

        self._heartbeat = HeartBeat.create(self, device_id)

        # Set up OpenOMCI environment
        self._onu_omci_device = None
        self._dev_info_loaded = False
        self._deferred = None

        self._in_sync_subscription = None
        self._port_state_subscription = None
        self._connectivity_subscription = None
        self._capabilities_subscription = None

        self.mac_bridge_service_profile_entity_id = 0x201
        self.gal_enet_profile_entity_id = 0x1

        self._tp_service_specific_task = dict()
        self._tech_profile_download_done = dict()

        # When the vlan filter is being removed for a given TP ID on a given UNI,
        # mark that we are expecting a tp delete to happen for this UNI.
        # Unless the TP delete is complete to not allow new vlan add tasks to this TP ID
        self._pending_delete_tp = dict()

        # Stores information related to queued vlan filter tasks
        # Dictionary with key being uni_id and value being device,uni port ,uni id and vlan id
        self._queued_vlan_filter_task = dict()

        self._set_vlan = dict()  # uni_id, tp_id -> set_vlan_id

        # Paths from kv store
        ONU_PATH = 'service/voltha/openonu'

        # Initialize KV store client
        self.args = registry('main').get_args()
        host, port = self.args.etcd.split(':', 1)
        self.tp_kv_client = TwistedEtcdStore(host, port, TechProfile.KV_STORE_TECH_PROFILE_PATH_PREFIX)
        self.onu_kv_client = TwistedEtcdStore(host, port, ONU_PATH)

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, value):
        if self._enabled != value:
            self._enabled = value

    @property
    def omci_agent(self):
        return self.adapter.omci_agent

    @property
    def omci_cc(self):
        return self._onu_omci_device.omci_cc if self._onu_omci_device is not None else None

    @property
    def heartbeat(self):
        return self._heartbeat

    @property
    def uni_ports(self):
        return list(self._unis.values())

    def uni_port(self, port_no_or_name):
        if isinstance(port_no_or_name, six.string_types):
            return next((uni for uni in self.uni_ports
                         if uni.name == port_no_or_name), None)

        assert isinstance(port_no_or_name, int), 'Invalid parameter type'
        return next((uni for uni in self.uni_ports
                     if uni.port_number == port_no_or_name), None)

    @property
    def pon_port(self):
        return self._pon

    @property
    def onu_omci_device(self):
        return self._onu_omci_device

    def receive_message(self, msg):
        if self.omci_cc is not None:
            self.omci_cc.receive_message(msg)

    def get_ofp_port_info(self, device, port_no):
        self.log.debug('get-ofp-port-info', port_no=port_no, device_id=device.id)
        cap = OFPPF_1GB_FD | OFPPF_FIBER

        hw_addr = mac_str_to_tuple('08:%02x:%02x:%02x:%02x:%02x' %
                                   ((device.parent_port_no >> 8 & 0xff),
                                    device.parent_port_no & 0xff,
                                    (port_no >> 16) & 0xff,
                                    (port_no >> 8) & 0xff,
                                    port_no & 0xff))

        uni_port = self.uni_port(int(port_no))
        name = device.serial_number + '-' + str(uni_port.mac_bridge_port_num)
        self.log.debug('ofp-port-name', port_no=port_no, name=name, uni_port=uni_port)

        ofstate = OFPPS_LINK_DOWN
        if uni_port.operstatus is OperStatus.ACTIVE:
            ofstate = OFPPS_LIVE

        return PortCapability(
            port=LogicalPort(
                ofp_port=ofp_port(
                    name=name,
                    hw_addr=hw_addr,
                    config=0,
                    state=ofstate,
                    curr=cap,
                    advertised=cap,
                    peer=cap,
                    curr_speed=OFPPF_1GB_FD,
                    max_speed=OFPPF_1GB_FD
                ),
                device_id=device.id,
                device_port_no=port_no
            )
        )

    # Called once when the adapter creates the device/onu instance
    @inlineCallbacks
    def activate(self, device):
        self.log.debug('activate-device', device_id=device.id, serial_number=device.serial_number)

        assert device.parent_id
        assert device.parent_port_no
        assert device.proxy_address.device_id

        self.proxy_address = device.proxy_address
        self.parent_id = device.parent_id
        self._pon_port_number = device.parent_port_no
        if self.enabled is not True:
            self.log.info('activating-new-onu', device_id=device.id, serial_number=device.serial_number)
            # populate what we know.  rest comes later after mib sync
            device.root = False
            device.vendor = 'OpenONU'
            device.reason = 'activating-onu'

            # TODO NEW CORE:  Need to either get logical device id from core or use regular device id
            # pm_metrics requires a logical device id.  For now set to just device_id
            self.logical_device_id = self.device_id

            self._onu_persisted_state['serial_number'] = device.serial_number
            try:
                self.log.debug('updating-onu-state', device_id=self.device_id,
                               onu_persisted_state=self._onu_persisted_state)
                yield self.onu_kv_client.set(self.device_id, json.dumps(self._onu_persisted_state))
            except Exception as e:
                self.log.error('could-not-store-onu-state', device_id=self.device_id,
                               onu_persisted_state=self._onu_persisted_state, e=e)
                # if we cannot write to storage we can proceed, for now.
                # later onu indications from the olt will have another chance

            yield self.core_proxy.device_update(device)
            self.log.debug('device-updated', device_id=device.id, serial_number=device.serial_number)

            yield self._init_pon_state()
            self.log.debug('pon state initialized', device_id=device.id, serial_number=device.serial_number)

            yield self._init_metrics()
            self.log.debug('metrics initialized', device_id=device.id, serial_number=device.serial_number)

            self.enabled = True
        else:
            self.log.info('onu-already-activated')

    # Called once when the adapter needs to re-create device.  usually on vcore restart
    @inlineCallbacks
    def reconcile(self, device):
        self.log.debug('reconcile-device', device_id=device.id, serial_number=device.serial_number)

        if self._reconciling:
            self.log.debug('already-running-reconcile-device', device_id=device.id, serial_number=device.serial_number)
            return

        # first we verify that we got parent reference and proxy info
        assert device.parent_id
        assert device.proxy_address.device_id

        self.proxy_address = device.proxy_address
        self.parent_id = device.parent_id
        self._pon_port_number = device.parent_port_no

        if self.enabled is not True:
            self._reconciling = True
            self.log.info('reconciling-openonu-device')
            self.logical_device_id = self.device_id

            try:
                query_data = yield self.onu_kv_client.get(device.id)
                self._onu_persisted_state = json.loads(query_data)
                self.log.debug('restored-onu-state', device_id=self.device_id,
                               onu_persisted_state=self._onu_persisted_state)
            except Exception as e:
                self.log.error('no-stored-onu-state', device_id=device.id, e=e)
                # there is nothing we can do without data.  flag the device as UNKNOWN and cannot reconcile
                # likely it will take manual steps to delete/re-add this onu
                yield self.core_proxy.device_reason_update(self.device_id, "cannot-reconcile")
                yield self.core_proxy.device_state_update(self.device_id, oper_status=OperStatus.UNKNOWN)
                return

            self._init_pon_state()
            self.log.debug('pon state initialized', device_id=device.id, serial_number=device.serial_number)

            self._init_metrics()
            self.log.debug('metrics initialized', device_id=device.id, serial_number=device.serial_number)

            self._subscribe_to_events()
            # need to restart omci start machines and reload mib database.  once db is loaded we can finish reconcile
            self._onu_omci_device.start(device)
            self._heartbeat.enabled = True

            self.enabled = True
        else:
            self.log.info('onu-already-activated')

    @inlineCallbacks
    def _init_pon_state(self):
        self.log.debug('init-pon-state', device_id=self.device_id, device_logical_id=self.logical_device_id)

        self._pon = PonPort.create(self, self._pon_port_number)
        self._pon.add_peer(self.parent_id, self._pon_port_number)
        self.log.debug('adding-pon-port-to-agent',
                       type=self._pon.get_port().type,
                       admin_state=self._pon.get_port().admin_state,
                       oper_status=self._pon.get_port().oper_status,
                       )

        if not self._reconciling:
            yield self.core_proxy.port_created(self.device_id, self._pon.get_port())

        self.log.debug('added-pon-port-to-agent',
                       type=self._pon.get_port().type,
                       admin_state=self._pon.get_port().admin_state,
                       oper_status=self._pon.get_port().oper_status,
                       )

        # Create and start the OpenOMCI ONU Device Entry for this ONU
        self._onu_omci_device = self.omci_agent.add_device(self.device_id,
                                                           self.core_proxy,
                                                           self.adapter_proxy,
                                                           support_classes=self.adapter.broadcom_omci,
                                                           custom_me_map=self.adapter.custom_me_entities())
        # Port startup
        if self._pon is not None:
            self._pon.enabled = True

    @inlineCallbacks
    def _init_metrics(self):
        self.log.debug('init-metrics', device_id=self.device_id, device_logical_id=self.logical_device_id)

        serial_number = self._onu_persisted_state.get('serial_number')

        ############################################################################
        # Setup Alarm handler
        self.events = AdapterEvents(self.core_proxy, self.device_id, self.logical_device_id,
                                    serial_number)
        ############################################################################
        # Setup PM configuration for this device
        # Pass in ONU specific options
        kwargs = {
            OnuPmMetrics.DEFAULT_FREQUENCY_KEY: OnuPmMetrics.DEFAULT_ONU_COLLECTION_FREQUENCY,
            'heartbeat': self.heartbeat,
            OnuOmciPmMetrics.OMCI_DEV_KEY: self._onu_omci_device
        }
        self.log.debug('create-pm-metrics', device_id=self.device_id, serial_number=serial_number)
        self._pm_metrics = OnuPmMetrics(self.events, self.core_proxy, self.device_id,
                                        self.logical_device_id, serial_number,
                                        grouped=True, freq_override=False, **kwargs)
        pm_config = self._pm_metrics.make_proto()
        self._onu_omci_device.set_pm_config(self._pm_metrics.omci_pm.openomci_interval_pm)
        self.log.debug("initial-pm-config", device_id=self.device_id, serial_number=serial_number)

        if not self._reconciling:
            yield self.core_proxy.device_pm_config_update(pm_config, init=True)

        # Note, ONU ID and UNI intf set in add_uni_port method
        self._onu_omci_device.alarm_synchronizer.set_alarm_params(mgr=self.events,
                                                                  ani_ports=[self._pon])

        # Code to Run OMCI Test Action
        kwargs_omci_test_action = {
            OmciTestRequest.DEFAULT_FREQUENCY_KEY:
                OmciTestRequest.DEFAULT_COLLECTION_FREQUENCY
        }
        self._test_request = OmciTestRequest(self.core_proxy,
                                             self.omci_agent, self.device_id,
                                             AniG, serial_number,
                                             self.logical_device_id,
                                             exclusive=False,
                                             **kwargs_omci_test_action)

    @inlineCallbacks
    def delete(self, device):
        self.log.info('delete-onu', device_id=device.id, serial_number=device.serial_number)
        try:
            yield self.onu_kv_client.delete(device.id)
        except Exception as e:
            self.log.error('could-not-delete-onu-state', device_id=device.id, e=e)

        try:
            self._deferred.cancel()
            self._test_request.stop_collector()
            self._pm_metrics.stop_collector()
            self.log.debug('removing-openomci-statemachine')
            self.omci_agent.remove_device(device.id, cleanup=True)
            yield self.onu_deleted_event()
        except Exception as e:
            self.log.error('could-not-delete-onu', device_id=device.id, e=e)

    def _create_tconts(self, uni_id, us_scheduler):
        alloc_id = us_scheduler['alloc_id']
        q_sched_policy = us_scheduler['q_sched_policy']
        self.log.debug('create-tcont', us_scheduler=us_scheduler)
        # TODO: revisit for multi tconts support
        new_tconts = []
        tcontdict = dict()
        tcontdict['alloc-id'] = alloc_id
        tcontdict['q_sched_policy'] = q_sched_policy
        tcontdict['uni_id'] = uni_id

        tcont = OnuTCont.create(self, tcont=tcontdict)

        success = self._pon.add_tcont(tcont)
        if success:
            new_tconts.append(tcont)
            self.log.debug('pon-add-tcont', tcont=tcont)

        return new_tconts

    # Called when there is an olt up indication, providing the gem port id chosen by the olt handler
    def _create_gemports(self, uni_id, gem_ports, alloc_id_ref, direction):
        self.log.debug('create-gemport',
                       gem_ports=gem_ports, direction=direction)
        new_gem_ports = []
        for gem_port in gem_ports:
            gemdict = dict()
            if gem_port[IS_MULTICAST] == 'True':
                gemdict[GEM_PORT_ID] = gem_port['multicast_gem_id']
                gemdict[IS_MULTICAST] = True
            else:
                gemdict[GEM_PORT_ID] = gem_port[GEM_PORT_ID]
                gemdict[IS_MULTICAST] = False

            gemdict['direction'] = direction
            gemdict['alloc_id_ref'] = alloc_id_ref
            gemdict['encryption'] = gem_port['aes_encryption']
            gemdict['discard_config'] = dict()
            gemdict['discard_config']['max_probability'] = \
                gem_port['discard_config']['max_probability']
            gemdict['discard_config']['max_threshold'] = \
                gem_port['discard_config']['max_threshold']
            gemdict['discard_config']['min_threshold'] = \
                gem_port['discard_config']['min_threshold']
            gemdict['discard_policy'] = gem_port['discard_policy']
            gemdict['max_q_size'] = gem_port['max_q_size']
            gemdict['pbit_map'] = gem_port['pbit_map']
            gemdict['priority_q'] = gem_port['priority_q']
            gemdict['scheduling_policy'] = gem_port['scheduling_policy']
            gemdict['weight'] = gem_port['weight']
            gemdict['uni_id'] = uni_id

            gem_port = OnuGemPort.create(self, gem_port=gemdict)

            success = self._pon.add_gem_port(gem_port, True)
            if success:
                new_gem_ports.append(gem_port)
                self.log.debug('pon-add-gemport', gem_port=gem_port)

        return new_gem_ports

    def _execute_queued_vlan_filter_tasks(self, uni_id, tp_id):
        # During OLT Reboots, ONU Reboots, ONU Disable/Enable, it is seen that vlan_filter
        # task is scheduled even before tp task. So we queue vlan-filter task if tp_task
        # or initial-mib-download is not done. Once the tp_task is completed, we execute
        # such queued vlan-filter tasks
        try:
            if uni_id in self._queued_vlan_filter_task and tp_id in self._queued_vlan_filter_task[uni_id]:
                self.log.info("executing-queued-vlan-filter-task",
                              uni_id=uni_id, tp_id=tp_id)
                for filter_info in self._queued_vlan_filter_task[uni_id][tp_id]:
                    reactor.callLater(0, self._add_vlan_filter_task, filter_info.get("device"),
                                      uni_id=uni_id, uni_port=filter_info.get("uni_port"),
                                      match_vlan=filter_info.get("match_vlan"),
                                      _set_vlan_vid=filter_info.get("set_vlan_vid"),
                                      _set_vlan_pcp=filter_info.get("set_vlan_pcp"),
                                      tp_id=filter_info.get("tp_id"))
                    # Now remove the entry from the dictionary
                    self.log.debug("executed-queued-vlan-filter-task",
                                   uni_id=uni_id, tp_id=tp_id)

                # Now delete the key entry for the tp_id once we have handled the
                # queued vlan filter tasks for that tp_id
                del self._queued_vlan_filter_task[uni_id][tp_id]
                # If the queued vlan filter tasks for all the tp_ids on a given
                # uni_id is handled, then delete the uni_id key
                if len(self._queued_vlan_filter_task[uni_id]) == 0:
                    del self._queued_vlan_filter_task[uni_id]
        except Exception as e:
            self.log.error("vlan-filter-configuration-failed", uni_id=uni_id, error=e)

    def _do_tech_profile_configuration(self, uni_id, tp):
        us_scheduler = tp['us_scheduler']
        alloc_id = us_scheduler['alloc_id']
        new_tconts = self._create_tconts(uni_id, us_scheduler)
        upstream_gem_port_attribute_list = tp['upstream_gem_port_attribute_list']
        new_upstream_gems = self._create_gemports(uni_id, upstream_gem_port_attribute_list, alloc_id, "UPSTREAM")
        downstream_gem_port_attribute_list = tp['downstream_gem_port_attribute_list']
        new_downstream_gems = self._create_gemports(uni_id, downstream_gem_port_attribute_list, alloc_id, "DOWNSTREAM")

        new_gems = []
        new_gems.extend(new_upstream_gems)
        new_gems.extend(new_downstream_gems)

        return new_tconts, new_gems

    @inlineCallbacks
    def load_and_configure_tech_profile(self, uni_id, tp_path):
        self.log.debug("loading-tech-profile-configuration", uni_id=uni_id, tp_path=tp_path)
        tp_id = self.extract_tp_id_from_path(tp_path)
        if uni_id not in self._tp_service_specific_task:
            self._tp_service_specific_task[uni_id] = dict()

        if uni_id not in self._tech_profile_download_done:
            self._tech_profile_download_done[uni_id] = dict()

        if tp_id not in self._tech_profile_download_done[uni_id]:
            self._tech_profile_download_done[uni_id][tp_id] = False

        if not self._tech_profile_download_done[uni_id][tp_id]:
            try:
                if tp_path in self._tp_service_specific_task[uni_id]:
                    self.log.info("tech-profile-config-already-in-progress",
                                  tp_path=tp_path)
                    returnValue(None)

                tpstored = yield self.tp_kv_client.get(tp_path)
                tpstring = tpstored.decode('ascii')
                tp = json.loads(tpstring)
                self._tp[tp_id] = tp
                self.log.debug("tp-instance", tp=tp)
                tconts, gem_ports = self._do_tech_profile_configuration(uni_id, tp)

                @inlineCallbacks
                def success(_results):
                    self.log.info("tech-profile-config-done-successfully", uni_id=uni_id, tp_id=tp_id)
                    if tp_path in self._tp_service_specific_task[uni_id]:
                        del self._tp_service_specific_task[uni_id][tp_path]
                    self._tech_profile_download_done[uni_id][tp_id] = True
                    # Now execute any vlan filter tasks that were queued for later
                    reactor.callInThread(self._execute_queued_vlan_filter_tasks, uni_id, tp_id)
                    yield self.core_proxy.device_reason_update(self.device_id, 'tech-profile-config-download-success')

                    # Execute mcast task
                    for gem in gem_ports:
                        self.log.debug("checking-multicast-service-for-gem ", gem=gem)
                        if gem.mcast is True:
                            self.log.info("found-multicast-service-for-gem ", gem=gem, uni_id=uni_id, tp_id=tp_id)
                            reactor.callInThread(self.start_multicast_service, uni_id, tp_path)
                            self.log.debug("started_multicast_service-successfully", tconts=tconts, gems=gem_ports)
                            break

                @inlineCallbacks
                def failure(_reason):
                    self.log.warn('tech-profile-config-failure-retrying', uni_id=uni_id, tp_id=tp_id,
                                  _reason=_reason)
                    if tp_path in self._tp_service_specific_task[uni_id]:
                        del self._tp_service_specific_task[uni_id][tp_path]
                    retry = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
                    reactor.callLater(retry, self.load_and_configure_tech_profile,
                                      uni_id, tp_path)
                    yield self.core_proxy.device_reason_update(self.device_id,
                                                               'tech-profile-config-download-failure-retrying')

                self.log.info('downloading-tech-profile-configuration', uni_id=uni_id, tp_id=tp_id)
                self.log.debug("tconts-gems-to-install", tconts=tconts, gem_ports=gem_ports)

                self.log.debug("current-cached-tconts", tconts=list(self.pon_port.tconts.values()))
                self.log.debug("current-cached-gem-ports", gem_ports=list(self.pon_port.gem_ports.values()))

                self._tp_service_specific_task[uni_id][tp_path] = \
                    BrcmTpSetupTask(self.omci_agent, self, uni_id, tconts, gem_ports, tp_id)
                self._deferred = \
                    self._onu_omci_device.task_runner.queue_task(self._tp_service_specific_task[uni_id][tp_path])
                self._deferred.addCallbacks(success, failure)

            except Exception as e:
                self.log.exception("error-loading-tech-profile", e=e)
        else:
            # There is an active tech-profile task ongoing on this UNI port. So, reschedule this task
            # after a short interval
            if uni_id in self._tp_service_specific_task and len(self._tp_service_specific_task[uni_id]):
                self.log.debug("active-tp-tasks-in-progress-for-uni--scheduling-this-task-for-later",
                               uni_id=uni_id, tp_path=tp_path)
                reactor.callLater(0.2, self.load_and_configure_tech_profile,
                                  uni_id, tp_path)
                return

            self.log.info("tech-profile-config-already-done")

            # Could be a case where TP exists but new gem-ports are getting added dynamically
            tpstored = yield self.tp_kv_client.get(tp_path)
            tpstring = tpstored.decode('ascii')
            tp = json.loads(tpstring)
            upstream_gems = []
            downstream_gems = []
            # Find out the new Gem ports that are getting added afresh.
            for gp in tp['upstream_gem_port_attribute_list']:
                if self.pon_port.gem_port(gp['gemport_id'], "upstream"):
                    # gem port already exists
                    continue
                upstream_gems.append(gp)
            for gp in tp['downstream_gem_port_attribute_list']:
                if self.pon_port.gem_port(gp['gemport_id'], "downstream"):
                    # gem port already exists
                    continue
                downstream_gems.append(gp)

            us_scheduler = tp['us_scheduler']
            alloc_id = us_scheduler['alloc_id']

            if len(upstream_gems) > 0 or len(downstream_gems) > 0:
                self.log.info("installing-new-gem-ports", upstream_gems=upstream_gems, downstream_gems=downstream_gems)
                new_upstream_gems = self._create_gemports(uni_id, upstream_gems, alloc_id, "UPSTREAM")
                new_downstream_gems = self._create_gemports(uni_id, downstream_gems, alloc_id, "DOWNSTREAM")
                new_gems = []
                new_gems.extend(new_upstream_gems)
                new_gems.extend(new_downstream_gems)

                def success(_results):
                    self.log.info("new-gem-ports-successfully-installed", result=_results)

                def failure(_reason):
                    self.log.warn('new-gem-port-install-failed--retrying',
                                  _reason=_reason)
                    # Remove gem ports from cache. We will re-add them during the retry
                    for gp in new_gems:
                        self.pon_port.remove_gem_id(gp.gem_id, gp.direction, False)

                    retry = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
                    reactor.callLater(retry, self.load_and_configure_tech_profile,
                                      uni_id, tp_path)

                self._tp_service_specific_task[uni_id][tp_path] = \
                    BrcmTpSetupTask(self.omci_agent, self, uni_id, [], new_gems, tp_id)
                self._deferred = \
                    self._onu_omci_device.task_runner.queue_task(self._tp_service_specific_task[uni_id][tp_path])
                self._deferred.addCallbacks(success, failure)

    @inlineCallbacks
    def start_multicast_service(self, uni_id, tp_path, retry_count=0):
        self.log.debug("starting-multicast-service", uni_id=uni_id, tp_path=tp_path)
        tp_id = self.extract_tp_id_from_path(tp_path)
        if uni_id in self._set_vlan and tp_id in self._set_vlan[uni_id]:
            try:
                tp = self._tp[tp_id]
                if tp is None:
                    tpstored = yield self.tp_kv_client.get(tp_path)
                    tpstring = tpstored.decode('ascii')
                    tp = json.loads(tpstring)
                    if tp is None:
                        self.log.error("cannot-find-tp-to-start-multicast-service", uni_id=uni_id, tp_path=tp_path)
                        return
                    else:
                        self._tp[tp_id] = tp

                self.log.debug("mcast-vlan-learned-before", self._set_vlan[uni_id][tp_id], uni_id=uni_id, tp_id=tp_id)

                def success(_results):
                    self.log.debug('multicast-success', uni_id=uni_id)
                    self._multicast_task = None

                def failure(_reason):
                    self.log.warn('multicast-failure', _reason=_reason)
                    retry = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
                    reactor.callLater(retry, self.start_multicast_service,
                                      uni_id, tp_path)

                self.log.debug('starting-multicast-task', mcast_vlan_id=self._set_vlan[uni_id][tp_id])
                downstream_gem_port_attribute_list = tp['downstream_gem_port_attribute_list']
                for i in range(len(downstream_gem_port_attribute_list)):
                    if IS_MULTICAST in downstream_gem_port_attribute_list[i] and \
                            downstream_gem_port_attribute_list[i][IS_MULTICAST] == 'True':
                        dynamic_access_control_list_table = downstream_gem_port_attribute_list[i][
                            'dynamic_access_control_list'].split("-")
                        static_access_control_list_table = downstream_gem_port_attribute_list[i][
                            'static_access_control_list'].split("-")
                        multicast_gem_id = downstream_gem_port_attribute_list[i]['multicast_gem_id']
                        break

                self._multicast_task = BrcmMcastTask(self.omci_agent, self, self.device_id, uni_id, tp_id,
                                                     self._set_vlan[uni_id][tp_id], dynamic_access_control_list_table,
                                                     static_access_control_list_table, multicast_gem_id)
                self._deferred = self._onu_omci_device.task_runner.queue_task(self._multicast_task)
                self._deferred.addCallbacks(success, failure)
            except Exception as e:
                self.log.exception("error-loading-multicast", e=e)
        else:
            if retry_count < 30:
                retry_count = +1
                self.log.debug("going-to-wait-for-flow-to-learn-mcast-vlan", uni_id=uni_id, tp_id=tp_id,
                               retry=retry_count)
                reactor.callLater(0.5, self.start_multicast_service, uni_id, tp_path, retry_count)
            else:
                self.log.error("mcast-vlan-not-configured-yet-failing-mcast-service-conf", uni_id=uni_id, tp_id=tp_id,
                               retry=retry_count)

    def delete_tech_profile(self, uni_id, tp_path, alloc_id=None, gem_port_id=None):
        try:
            tp_table_id = self.extract_tp_id_from_path(tp_path)
            if not uni_id in self._tech_profile_download_done:
                self.log.warn("tp-key-is-not-present", uni_id=uni_id)
                return

            if not tp_table_id in self._tech_profile_download_done[uni_id]:
                self.log.warn("tp-id-is-not-present", uni_id=uni_id, tp_id=tp_table_id)
                return

            if self._tech_profile_download_done[uni_id][tp_table_id] is not True:
                self.log.error("tp-download-is-not-done-in-order-to-process-tp-delete", uni_id=uni_id,
                               tp_id=tp_table_id)
                return

            if alloc_id is None and gem_port_id is None:
                self.log.error("alloc-id-and-gem-port-id-are-none", uni_id=uni_id, tp_id=tp_table_id)
                return

            # Extract the current set of TCONT and GEM Ports from the Handler's pon_port that are
            # relevant to this task's UNI. It won't change. But, the underlying pon_port may change
            # due to additional tasks on different UNIs. So, it we cannot use the pon_port affter
            # this initializer
            tcont = None
            self.log.debug("current-cached-tconts", tconts=list(self.pon_port.tconts.values()))
            for tc in list(self.pon_port.tconts.values()):
                if tc.alloc_id == alloc_id:
                    tcont = tc
                    self.pon_port.remove_tcont(tc.alloc_id, False)

            gem_port = None
            self.log.debug("current-cached-gem-ports", gem_ports=list(self.pon_port.gem_ports.values()))
            for gp in list(self.pon_port.gem_ports.values()):
                if gp.gem_id == gem_port_id:
                    gem_port = gp
                    self.pon_port.remove_gem_id(gp.gem_id, gp.direction, False)

            @inlineCallbacks
            def success(_results):
                if gem_port_id:
                    self.log.info("gem-port-delete-done-successfully")
                if alloc_id:
                    self.log.info("tcont-delete-done-successfully")
                    # The deletion of TCONT marks the complete deletion of tech-profile
                    try:
                        del self._tech_profile_download_done[uni_id][tp_table_id]
                        self.log.debug("tp-profile-download-flag-cleared", uni_id=uni_id, tp_id=tp_table_id)
                        del self._tp_service_specific_task[uni_id][tp_path]
                        self.log.debug("tp-service-specific-task-cleared", uni_id=uni_id, tp_id=tp_table_id)
                        del self._pending_delete_tp[uni_id][tp_table_id]
                        self.log.debug("pending-delete-tp-task-flag-cleared", uni_id=uni_id, tp_id=tp_table_id)
                    except Exception as ex:
                        self.log.error("del-tp-state-info", e=ex)

                # TODO: There could be multiple TP on the UNI, and also the ONU.
                # TODO: But the below reason updates for the whole device.
                yield self.core_proxy.device_reason_update(self.device_id, 'tech-profile-config-delete-success')

            @inlineCallbacks
            def failure(_reason):
                self.log.warn('tech-profile-delete-failure-retrying',
                              _reason=_reason)
                retry = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
                reactor.callLater(retry, self.delete_tech_profile, uni_id, tp_path, alloc_id, gem_port_id)
                yield self.core_proxy.device_reason_update(self.device_id,
                                                           'tech-profile-config-delete-failure-retrying')

            self.log.info('deleting-tech-profile-configuration')

            if tcont is None and gem_port is None:
                if alloc_id is not None:
                    self.log.error("tcont-info-corresponding-to-alloc-id-not-found", alloc_id=alloc_id)
                if gem_port_id is not None:
                    self.log.error("gem-port-info-corresponding-to-gem-port-id-not-found", gem_port_id=gem_port_id)
                return

            self._tp_service_specific_task[uni_id][tp_path] = \
                BrcmTpDeleteTask(self.omci_agent, self, uni_id, tp_table_id,
                                 tcont=tcont, gem_port=gem_port)
            self._deferred = \
                self._onu_omci_device.task_runner.queue_task(self._tp_service_specific_task[uni_id][tp_path])
            self._deferred.addCallbacks(success, failure)
        except Exception as e:
            self.log.exception("failed-to-delete-tp",
                               e=e, uni_id=uni_id, tp_path=tp_path,
                               alloc_id=alloc_id, gem_port_id=gem_port_id)

    def update_pm_config(self, device, pm_configs):
        # TODO: This has not been tested
        self.log.info('update_pm_config', pm_configs=pm_configs)
        self._pm_metrics.update(pm_configs)

    def remove_onu_flows(self, device, flows):
        self.log.debug('remove-onu-flows')

        # no point in removing omci flows if the device isnt reachable
        if device.connect_status != ConnectStatus.REACHABLE or \
                device.admin_state != AdminState.ENABLED:
            self.log.warn("device-disabled-or-offline-skipping-remove-flow",
                          admin=device.admin_state, connect=device.connect_status)
            return

        for flow in flows:
            # if incoming flow contains cookie, then remove from ONU
            if flow.cookie:
                self.log.debug("remove-flow", device_id=device.id, flow=flow)

                def is_downstream(port):
                    return port == self._pon_port_number

                def is_upstream(port):
                    return not is_downstream(port)

                try:
                    _in_port = fd.get_in_port(flow)
                    assert _in_port is not None

                    _out_port = fd.get_out_port(flow)  # may be None

                    if is_downstream(_in_port):
                        self.log.debug('downstream-flow-no-need-to-remove', in_port=_in_port, out_port=_out_port,
                                       device_id=device.id)
                        # extended vlan tagging operation will handle it
                        continue
                    elif is_upstream(_in_port):
                        self.log.debug('upstream-flow', in_port=_in_port, out_port=_out_port)
                        if fd.is_dhcp_flow(flow):
                            self.log.debug('The dhcp trap-to-host flow will be discarded', device_id=device.id)
                            return

                        _match_vlan_vid = None
                        for field in fd.get_ofb_fields(flow):
                            if field.type == fd.VLAN_VID:
                                if field.vlan_vid == RESERVED_TRANSPARENT_VLAN and field.vlan_vid_mask == RESERVED_TRANSPARENT_VLAN:
                                    _match_vlan_vid = RESERVED_TRANSPARENT_VLAN
                                else:
                                    _match_vlan_vid = field.vlan_vid & 0xfff
                                self.log.debug('field-type-vlan-vid',
                                               vlan=_match_vlan_vid)

                        _set_vlan_vid = None
                        _set_vlan_pcp = None
                        # Retrieve the VLAN_VID that needs to be removed from the EVTO rule on the ONU.
                        for action in fd.get_actions(flow):
                            if action.type == fd.SET_FIELD:
                                _field = action.set_field.field.ofb_field
                                assert (action.set_field.field.oxm_class ==
                                        OFPXMC_OPENFLOW_BASIC)
                                if _field.type == fd.VLAN_VID:
                                    _set_vlan_vid = _field.vlan_vid & 0xfff
                                    self.log.debug('vlan-vid-to-remove',
                                                   _vlan_vid=_set_vlan_vid, in_port=_in_port)
                                elif _field.type == fd.VLAN_PCP:
                                    _set_vlan_pcp = _field.vlan_pcp
                                    self.log.debug('set-field-type-vlan-pcp',
                                                   vlan_pcp=_set_vlan_pcp)

                        uni_port = self.uni_port(_in_port)
                        uni_id = _in_port & 0xF
                    else:
                        raise Exception('port should be 1 or 2 by our convention')

                    self.log.debug('flow-ports', in_port=_in_port, out_port=_out_port, uni_port=str(uni_port))

                    tp_id = self.get_tp_id_in_flow(flow)
                    # The vlan filter remove should be followed by a TP deleted for that TP ID.
                    # Use this information to re-schedule any vlan filter add tasks for the same TP ID again.
                    # First check if the TP download was done, before we access that TP delete is necessary
                    if uni_id in self._tech_profile_download_done and tp_id in self._tech_profile_download_done[
                        uni_id] and \
                            self._tech_profile_download_done[uni_id][tp_id] is True:
                        if uni_id not in self._pending_delete_tp:
                            self._pending_delete_tp[uni_id] = dict()
                            self._pending_delete_tp[uni_id][tp_id] = True
                        else:
                            self._pending_delete_tp[uni_id][tp_id] = True
                    # Deleting flow from ONU.
                    self._remove_vlan_filter_task(device, uni_id, uni_port=uni_port,
                                                  _set_vlan_pcp=_set_vlan_pcp,
                                                  _set_vlan_vid=_set_vlan_vid,
                                                  match_vlan=_match_vlan_vid,
                                                  tp_id=tp_id)
                    # TODO:Delete TD task.
                except Exception as e:
                    self.log.exception('failed-to-remove-flow', e=e)

    def add_onu_flows(self, device, flows):
        self.log.debug('add-onu-flows')

        # no point in pushing omci flows if the device isnt reachable
        if device.connect_status != ConnectStatus.REACHABLE or \
                device.admin_state != AdminState.ENABLED:
            self.log.warn("device-disabled-or-offline-skipping-flow-update",
                          admin=device.admin_state, connect=device.connect_status)
            return

        def is_downstream(port):
            return port == self._pon_port_number

        def is_upstream(port):
            return not is_downstream(port)

        for flow in flows:
            # if incoming flow contains cookie, then add to ONU
            if flow.cookie:
                _type = None
                _port = None
                _vlan_vid = None
                _udp_dst = None
                _udp_src = None
                _ipv4_dst = None
                _ipv4_src = None
                _metadata = None
                _output = None
                _push_tpid = None
                _field = None
                _set_vlan_vid = None
                _set_vlan_pcp = None
                _tunnel_id = None
                self.log.debug("add-flow", device_id=device.id, flow=flow)

                try:
                    _in_port = fd.get_in_port(flow)
                    assert _in_port is not None

                    _out_port = fd.get_out_port(flow)  # may be None
                    tp_id = self.get_tp_id_in_flow(flow)
                    if is_downstream(_in_port):
                        self.log.debug('downstream-flow', in_port=_in_port, out_port=_out_port)
                        # NOTE: We don't care downstream flow because we will copy vlan_id to upstream flow
                        # uni_port = self.uni_port(_out_port)
                        # uni_id = _out_port  & 0xF
                        continue
                    elif is_upstream(_in_port):
                        self.log.debug('upstream-flow', in_port=_in_port, out_port=_out_port)
                        uni_port = self.uni_port(_in_port)
                        uni_id = _in_port & 0xF
                    else:
                        raise Exception('port should be 1 or 2 by our convention')

                    self.log.debug('flow-ports', in_port=_in_port, out_port=_out_port, uni_port=str(uni_port))

                    for field in fd.get_ofb_fields(flow):
                        if field.type == fd.ETH_TYPE:
                            _type = field.eth_type
                            self.log.debug('field-type-eth-type',
                                           eth_type=_type)

                        elif field.type == fd.IP_PROTO:
                            _proto = field.ip_proto
                            self.log.debug('field-type-ip-proto',
                                           ip_proto=_proto)

                        elif field.type == fd.IN_PORT:
                            _port = field.port
                            self.log.debug('field-type-in-port',
                                           in_port=_port)
                        elif field.type == fd.TUNNEL_ID:
                            self.log.debug('field-type-tunnel-id')

                        elif field.type == fd.VLAN_VID:
                            if field.vlan_vid == RESERVED_TRANSPARENT_VLAN and field.vlan_vid_mask == RESERVED_TRANSPARENT_VLAN:
                                _vlan_vid = RESERVED_TRANSPARENT_VLAN
                            else:
                                _vlan_vid = field.vlan_vid & 0xfff
                            self.log.debug('field-type-vlan-vid',
                                           vlan=_vlan_vid)

                        elif field.type == fd.VLAN_PCP:
                            _vlan_pcp = field.vlan_pcp
                            self.log.debug('field-type-vlan-pcp',
                                           pcp=_vlan_pcp)

                        elif field.type == fd.UDP_DST:
                            _udp_dst = field.udp_dst
                            self.log.debug('field-type-udp-dst',
                                           udp_dst=_udp_dst)

                        elif field.type == fd.UDP_SRC:
                            _udp_src = field.udp_src
                            self.log.debug('field-type-udp-src',
                                           udp_src=_udp_src)

                        elif field.type == fd.IPV4_DST:
                            _ipv4_dst = field.ipv4_dst
                            self.log.debug('field-type-ipv4-dst',
                                           ipv4_dst=_ipv4_dst)

                        elif field.type == fd.IPV4_SRC:
                            _ipv4_src = field.ipv4_src
                            self.log.debug('field-type-ipv4-src',
                                           ipv4_dst=_ipv4_src)

                        elif field.type == fd.METADATA:
                            _metadata = field.table_metadata
                            self.log.debug('field-type-metadata',
                                           metadata=_metadata)

                        else:
                            raise NotImplementedError('field.type={}'.format(
                                field.type))

                    for action in fd.get_actions(flow):

                        if action.type == fd.OUTPUT:
                            _output = action.output.port
                            self.log.debug('action-type-output',
                                           output=_output, in_port=_in_port)

                        elif action.type == fd.POP_VLAN:
                            self.log.debug('action-type-pop-vlan',
                                           in_port=_in_port)

                        elif action.type == fd.PUSH_VLAN:
                            _push_tpid = action.push.ethertype
                            self.log.debug('action-type-push-vlan',
                                           push_tpid=_push_tpid, in_port=_in_port)
                            if action.push.ethertype != 0x8100:
                                self.log.error('unhandled-tpid',
                                               ethertype=action.push.ethertype)

                        elif action.type == fd.SET_FIELD:
                            _field = action.set_field.field.ofb_field
                            assert (action.set_field.field.oxm_class ==
                                    OFPXMC_OPENFLOW_BASIC)
                            self.log.debug('action-type-set-field',
                                           field=_field, in_port=_in_port)
                            if _field.type == fd.VLAN_VID:
                                _set_vlan_vid = _field.vlan_vid & 0xfff
                                self.log.debug('set-field-type-vlan-vid',
                                               vlan_vid=_set_vlan_vid)
                            elif _field.type == fd.VLAN_PCP:
                                _set_vlan_pcp = _field.vlan_pcp
                                self.log.debug('set-field-type-vlan-pcp',
                                               vlan_pcp=_set_vlan_pcp)
                            else:
                                self.log.error('unsupported-action-set-field-type',
                                               field_type=_field.type)
                        else:
                            self.log.error('unsupported-action-type',
                                           action_type=action.type, in_port=_in_port)

                    if self._set_vlan is not None:
                        if uni_id not in self._set_vlan:
                            self._set_vlan[uni_id] = dict()
                        self._set_vlan[uni_id][tp_id] = _set_vlan_vid
                        self.log.debug("set_vlan_id-for-tp", _set_vlan_vid=_set_vlan_vid, tp_id=tp_id)

                    # OMCI set vlan task can only filter and set on vlan header attributes.  Any other openflow
                    # supported match and action criteria cannot be handled by omci and must be ignored.
                    if (_set_vlan_vid is None or _set_vlan_vid == 0) and _vlan_vid != RESERVED_TRANSPARENT_VLAN:
                        self.log.warn('ignoring-flow-that-does-not-set-vlanid', set_vlan_vid=_set_vlan_vid)
                    elif (_set_vlan_vid is None or _set_vlan_vid == 0) and _vlan_vid == RESERVED_TRANSPARENT_VLAN:
                        self.log.info('set-vlanid-any', uni_id=uni_id, uni_port=uni_port,
                                      _set_vlan_vid=_vlan_vid,
                                      _set_vlan_pcp=_set_vlan_pcp, match_vlan=_vlan_vid,
                                      tp_id=tp_id)
                        self._add_vlan_filter_task(device, uni_id=uni_id, uni_port=uni_port,
                                                   _set_vlan_vid=_vlan_vid,
                                                   _set_vlan_pcp=_set_vlan_pcp, match_vlan=_vlan_vid,
                                                   tp_id=tp_id)
                    else:
                        self.log.info('set-vlanid', uni_id=uni_id, uni_port=uni_port, match_vlan=_vlan_vid,
                                      set_vlan_vid=_set_vlan_vid, _set_vlan_pcp=_set_vlan_pcp, ethType=_type)
                        self._add_vlan_filter_task(device, uni_id=uni_id, uni_port=uni_port,
                                                   _set_vlan_vid=_set_vlan_vid,
                                                   _set_vlan_pcp=_set_vlan_pcp, match_vlan=_vlan_vid,
                                                   tp_id=tp_id)

                except Exception as e:
                    self.log.exception('failed-to-install-flow', e=e, flow=flow)

    # Calling this assumes the onu is active/ready and had at least an initial mib downloaded.   This gets called from
    # flow decomposition that ultimately comes from onos
    def update_flow_table(self, device, flows):
        self.log.debug('update-flow-table', device_id=device.id, serial_number=device.serial_number)

        #
        # We need to proxy through the OLT to get to the ONU
        # Configuration from here should be using OMCI
        #
        # self.log.info('bulk-flow-update', device_id=device.id, flows=flows)

        # no point in pushing omci flows if the device isnt reachable
        if device.connect_status != ConnectStatus.REACHABLE or \
                device.admin_state != AdminState.ENABLED:
            self.log.warn("device-disabled-or-offline-skipping-flow-update",
                          admin=device.admin_state, connect=device.connect_status)
            return

        def is_downstream(port):
            return port == self._pon_port_number

        def is_upstream(port):
            return not is_downstream(port)

        for flow in flows:
            _type = None
            _port = None
            _vlan_vid = None
            _udp_dst = None
            _udp_src = None
            _ipv4_dst = None
            _ipv4_src = None
            _metadata = None
            _output = None
            _push_tpid = None
            _field = None
            _set_vlan_vid = None
            _set_vlan_pcp = None
            _tunnel_id = None

            try:
                write_metadata = fd.get_write_metadata(flow)
                if write_metadata is None:
                    self.log.error("do-not-process-flow-without-write-metadata")
                    return

                # extract tp id from flow
                tp_id = self.get_tp_id_in_flow(flow)
                self.log.debug("tp-id-in-flow", tp_id=tp_id)

                _in_port = fd.get_in_port(flow)
                assert _in_port is not None

                _out_port = fd.get_out_port(flow)  # may be None

                if is_downstream(_in_port):
                    self.log.debug('downstream-flow', in_port=_in_port, out_port=_out_port)
                    uni_port = self.uni_port(_out_port)
                    uni_id = _out_port & 0xF
                elif is_upstream(_in_port):
                    self.log.debug('upstream-flow', in_port=_in_port, out_port=_out_port)
                    uni_port = self.uni_port(_in_port)
                    uni_id = _in_port & 0xF
                else:
                    raise Exception('port should be 1 or 2 by our convention')

                self.log.debug('flow-ports', in_port=_in_port, out_port=_out_port, uni_port=str(uni_port))

                for field in fd.get_ofb_fields(flow):
                    if field.type == fd.ETH_TYPE:
                        _type = field.eth_type
                        self.log.debug('field-type-eth-type',
                                       eth_type=_type)

                    elif field.type == fd.IP_PROTO:
                        _proto = field.ip_proto
                        self.log.debug('field-type-ip-proto',
                                       ip_proto=_proto)

                    elif field.type == fd.IN_PORT:
                        _port = field.port
                        self.log.debug('field-type-in-port',
                                       in_port=_port)

                    elif field.type == fd.VLAN_VID:
                        if field.vlan_vid == RESERVED_TRANSPARENT_VLAN and field.vlan_vid_mask == RESERVED_TRANSPARENT_VLAN:
                            _vlan_vid = RESERVED_TRANSPARENT_VLAN
                        else:
                            _vlan_vid = field.vlan_vid & 0xfff
                        self.log.debug('field-type-vlan-vid',
                                       vlan=_vlan_vid)

                    elif field.type == fd.VLAN_PCP:
                        _vlan_pcp = field.vlan_pcp
                        self.log.debug('field-type-vlan-pcp',
                                       pcp=_vlan_pcp)

                    elif field.type == fd.UDP_DST:
                        _udp_dst = field.udp_dst
                        self.log.debug('field-type-udp-dst',
                                       udp_dst=_udp_dst)

                    elif field.type == fd.UDP_SRC:
                        _udp_src = field.udp_src
                        self.log.debug('field-type-udp-src',
                                       udp_src=_udp_src)

                    elif field.type == fd.IPV4_DST:
                        _ipv4_dst = field.ipv4_dst
                        self.log.debug('field-type-ipv4-dst',
                                       ipv4_dst=_ipv4_dst)

                    elif field.type == fd.IPV4_SRC:
                        _ipv4_src = field.ipv4_src
                        self.log.debug('field-type-ipv4-src',
                                       ipv4_dst=_ipv4_src)

                    elif field.type == fd.METADATA:
                        _metadata = field.table_metadata
                        self.log.debug('field-type-metadata',
                                       metadata=_metadata)

                    elif field.type == fd.TUNNEL_ID:
                        _tunnel_id = field.tunnel_id
                        self.log.debug('field-type-tunnel-id',
                                       tunnel_id=_tunnel_id)


                    else:
                        raise NotImplementedError('field.type={}'.format(
                            field.type))

                for action in fd.get_actions(flow):

                    if action.type == fd.OUTPUT:
                        _output = action.output.port
                        self.log.debug('action-type-output',
                                       output=_output, in_port=_in_port)

                    elif action.type == fd.POP_VLAN:
                        self.log.debug('action-type-pop-vlan',
                                       in_port=_in_port)

                    elif action.type == fd.PUSH_VLAN:
                        _push_tpid = action.push.ethertype
                        self.log.debug('action-type-push-vlan',
                                       push_tpid=_push_tpid, in_port=_in_port)
                        if action.push.ethertype != 0x8100:
                            self.log.error('unhandled-tpid',
                                           ethertype=action.push.ethertype)

                    elif action.type == fd.SET_FIELD:
                        _field = action.set_field.field.ofb_field
                        assert (action.set_field.field.oxm_class ==
                                OFPXMC_OPENFLOW_BASIC)
                        self.log.debug('action-type-set-field',
                                       field=_field, in_port=_in_port)
                        if _field.type == fd.VLAN_VID:
                            _set_vlan_vid = _field.vlan_vid & 0xfff
                            self.log.debug('set-field-type-vlan-vid',
                                           vlan_vid=_set_vlan_vid)
                        elif _field.type == fd.VLAN_PCP:
                            _set_vlan_pcp = _field.vlan_pcp
                            self.log.debug('set-field-type-vlan-pcp',
                                           vlan_pcp=_set_vlan_pcp)
                        else:
                            self.log.error('unsupported-action-set-field-type',
                                           field_type=_field.type)
                    else:
                        self.log.error('unsupported-action-type',
                                       action_type=action.type, in_port=_in_port)

                if self._set_vlan is not None:
                    if uni_id not in self._set_vlan:
                        self._set_vlan[uni_id] = dict()
                    self._set_vlan[uni_id][tp_id] = _set_vlan_vid
                    self.log.debug("set_vlan_id-for-tp", _set_vlan_vid=_set_vlan_vid, tp_id=tp_id)
                # OMCI set vlan task can only filter and set on vlan header attributes.  Any other openflow
                # supported match and action criteria cannot be handled by omci and must be ignored.
                if (_set_vlan_vid is None or _set_vlan_vid == 0) and _vlan_vid != RESERVED_TRANSPARENT_VLAN:
                    self.log.warn('ignoring-flow-that-does-not-set-vlanid', set_vlan_vid=_set_vlan_vid)
                elif (_set_vlan_vid is None or _set_vlan_vid == 0) and _vlan_vid == RESERVED_TRANSPARENT_VLAN:
                    self.log.info('set-vlanid-any', uni_id=uni_id, uni_port=uni_port,
                                  _set_vlan_vid=_vlan_vid,
                                  _set_vlan_pcp=_set_vlan_pcp, match_vlan=_vlan_vid,
                                  tp_id=tp_id)
                    self._add_vlan_filter_task(device, uni_id=uni_id, uni_port=uni_port,
                                               _set_vlan_vid=_vlan_vid,
                                               _set_vlan_pcp=_set_vlan_pcp, match_vlan=_vlan_vid,
                                               tp_id=tp_id)
                else:
                    self.log.info('set-vlanid', uni_id=uni_id, uni_port=uni_port, match_vlan=_vlan_vid,
                                  set_vlan_vid=_set_vlan_vid, _set_vlan_pcp=_set_vlan_pcp, ethType=_type)
                    self._add_vlan_filter_task(device, uni_id=uni_id, uni_port=uni_port,
                                               _set_vlan_vid=_set_vlan_vid,
                                               _set_vlan_pcp=_set_vlan_pcp, match_vlan=_vlan_vid,
                                               tp_id=tp_id)
            except Exception as e:
                self.log.exception('failed-to-install-flow', e=e, flow=flow)

    def _add_vlan_filter_task(self, device, uni_id, uni_port=None, match_vlan=0,
                              _set_vlan_vid=None, _set_vlan_pcp=8, tp_id=0):
        if uni_id in self._pending_delete_tp and tp_id in self._pending_delete_tp[uni_id] and \
                self._pending_delete_tp[uni_id][tp_id] is True:
            self.log.debug("pending-del-tp--scheduling-add-vlan-filter-task-for-later")
            reactor.callLater(0.2, self._add_vlan_filter_task, device, uni_id, uni_port, match_vlan,
                              _set_vlan_vid, _set_vlan_pcp, tp_id)
            return

        self.log.info('_adding_vlan_filter_task', uni_port=uni_port, uni_id=uni_id, tp_id=tp_id, match_vlan=match_vlan,
                      vlan=_set_vlan_vid, vlan_pcp=_set_vlan_pcp)
        assert uni_port is not None
        if uni_id in self._tech_profile_download_done and tp_id in self._tech_profile_download_done[uni_id] and \
                self._tech_profile_download_done[uni_id][tp_id] is True:
            @inlineCallbacks
            def success(_results):
                self.log.info('vlan-tagging-success', uni_port=uni_port, vlan=_set_vlan_vid, tp_id=tp_id,
                              set_vlan_pcp=_set_vlan_pcp)
                yield self.core_proxy.device_reason_update(self.device_id, 'omci-flows-pushed')

            @inlineCallbacks
            def failure(_reason):
                self.log.warn('vlan-tagging-failure', uni_port=uni_port, vlan=_set_vlan_vid, tp_id=tp_id)
                retry = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
                reactor.callLater(retry,
                                  self._add_vlan_filter_task, device, uni_id, uni_port=uni_port,
                                  match_vlan=match_vlan, _set_vlan_vid=_set_vlan_vid,
                                  _set_vlan_pcp=_set_vlan_pcp, tp_id=tp_id)
                yield self.core_proxy.device_reason_update(self.device_id, 'omci-flows-failed-retrying')

            self.log.info('setting-vlan-tag', uni_port=uni_port, uni_id=uni_id, tp_id=tp_id, match_vlan=match_vlan,
                          vlan=_set_vlan_vid, vlan_pcp=_set_vlan_pcp)
            vlan_filter_add_task = BrcmVlanFilterTask(self.omci_agent, self, uni_port, _set_vlan_vid,
                                                      match_vlan, _set_vlan_pcp, add_tag=True,
                                                      tp_id=tp_id)
            self._deferred = self._onu_omci_device.task_runner.queue_task(vlan_filter_add_task)
            self._deferred.addCallbacks(success, failure)
        else:
            self.log.info('tp-service-specific-task-not-done-adding-request-to-local-cache',
                          uni_id=uni_id, tp_id=tp_id)
            if uni_id not in self._queued_vlan_filter_task:
                self._queued_vlan_filter_task[uni_id] = dict()
            if tp_id not in self._queued_vlan_filter_task[uni_id]:
                self._queued_vlan_filter_task[uni_id][tp_id] = []
            self._queued_vlan_filter_task[uni_id][tp_id].append({"device": device,
                                                                 "uni_id": uni_id,
                                                                 "uni_port": uni_port,
                                                                 "match_vlan": match_vlan,
                                                                 "set_vlan_vid": _set_vlan_vid,
                                                                 "set_vlan_pcp": _set_vlan_pcp,
                                                                 "tp_id": tp_id})

    def get_tp_id_in_flow(self, flow):
        flow_metadata = fd.get_metadata_from_write_metadata(flow)
        tp_id = fd.get_tp_id_from_metadata(flow_metadata)
        return tp_id

    def _remove_vlan_filter_task(self, device, uni_id, uni_port=None, match_vlan=0,
                                 _set_vlan_vid=None, _set_vlan_pcp=8, tp_id=0):
        assert uni_port is not None

        @inlineCallbacks
        def success(_results):
            self.log.info('vlan-untagging-success', _results=_results)
            yield self.core_proxy.device_reason_update(self.device_id, 'omci-flows-deleted')

        @inlineCallbacks
        def failure(_reason):
            self.log.warn('vlan-untagging-failure', _reason=_reason)
            yield self.core_proxy.device_reason_update(self.device_id, 'omci-flows-deletion-failed-retrying')
            retry = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
            reactor.callLater(retry,
                              self._remove_vlan_filter_task, device, uni_id,
                              add_tag=False, uni_port=uni_port)

        self.log.info("remove_vlan_filter_task", tp_id=tp_id)
        vlan_remove_task = BrcmVlanFilterTask(self.omci_agent, self, uni_port, _set_vlan_vid,
                                              match_vlan, _set_vlan_pcp, add_tag=False,
                                              tp_id=tp_id)
        self._deferred = self._onu_omci_device.task_runner.queue_task(vlan_remove_task)
        self._deferred.addCallbacks(success, failure)

    @inlineCallbacks
    def process_inter_adapter_message(self, request):
        self.log.debug('process-inter-adapter-message', type=request.header.type, from_topic=request.header.from_topic,
                       to_topic=request.header.to_topic, to_device_id=request.header.to_device_id)

        if not self.enabled:
            self.log.warn('device-not-activated')
            reactor.callLater(0.5, self.process_inter_adapter_message, request)
            return

        try:

            update_onu_state = False

            if request.header.type == InterAdapterMessageType.OMCI_REQUEST:
                omci_msg = InterAdapterOmciMessage()
                request.body.Unpack(omci_msg)
                self.log.debug('inter-adapter-recv-omci', omci_msg=hexify(omci_msg.message))

                self.receive_message(omci_msg.message)

            elif request.header.type == InterAdapterMessageType.ONU_IND_REQUEST:
                onu_indication = OnuIndication()
                request.body.Unpack(onu_indication)
                self.log.debug('inter-adapter-recv-onu-ind', onu_id=onu_indication.onu_id,
                               oper_state=onu_indication.oper_state, admin_state=onu_indication.admin_state,
                               serial_number=onu_indication.serial_number)

                update_onu_state = True
                self._onu_persisted_state['onu_id'] = onu_indication.onu_id
                self._onu_persisted_state['intf_id'] = onu_indication.intf_id
                self._onu_persisted_state['admin_state'] = onu_indication.admin_state
                self._onu_persisted_state['oper_state'] = onu_indication.oper_state

                if onu_indication.oper_state == "up":
                    yield self.create_interface(onu_indication)
                elif onu_indication.oper_state == "down" or onu_indication.oper_state == "unreachable":
                    yield self.update_interface(onu_indication)
                else:
                    self.log.error("unknown-onu-indication", onu_id=onu_indication.onu_id,
                                   serial_number=onu_indication.serial_number)

            elif request.header.type == InterAdapterMessageType.TECH_PROFILE_DOWNLOAD_REQUEST:
                tech_msg = InterAdapterTechProfileDownloadMessage()
                request.body.Unpack(tech_msg)
                self.log.debug('inter-adapter-recv-tech-profile', tech_msg=tech_msg)

                update_onu_state = self._update_onu_persisted_state(tech_msg.uni_id, tp_path=tech_msg.path)
                yield self.load_and_configure_tech_profile(tech_msg.uni_id, tech_msg.path)

            elif request.header.type == InterAdapterMessageType.DELETE_GEM_PORT_REQUEST:
                del_gem_msg = InterAdapterDeleteGemPortMessage()
                request.body.Unpack(del_gem_msg)
                self.log.debug('inter-adapter-recv-del-gem', gem_del_msg=del_gem_msg)

                self.delete_tech_profile(uni_id=del_gem_msg.uni_id,
                                         gem_port_id=del_gem_msg.gem_port_id,
                                         tp_path=del_gem_msg.tp_path)

            elif request.header.type == InterAdapterMessageType.DELETE_TCONT_REQUEST:
                del_tcont_msg = InterAdapterDeleteTcontMessage()
                request.body.Unpack(del_tcont_msg)
                self.log.debug('inter-adapter-recv-del-tcont', del_tcont_msg=del_tcont_msg)

                # Removal of the tcont/alloc id mapping represents the removal of the tech profile
                update_onu_state = self._update_onu_persisted_state(del_tcont_msg.uni_id, tp_path=None)
                self.delete_tech_profile(uni_id=del_tcont_msg.uni_id,
                                         alloc_id=del_tcont_msg.alloc_id,
                                         tp_path=del_tcont_msg.tp_path)
            else:
                self.log.error("inter-adapter-unhandled-type", request=request)

            if update_onu_state:
                try:
                    self.log.debug('updating-onu-state', device_id=self.device_id,
                                   onu_persisted_state=self._onu_persisted_state)
                    yield self.onu_kv_client.set(self.device_id, json.dumps(self._onu_persisted_state))
                except Exception as e:
                    self.log.error('could-not-store-onu-state', device_id=self.device_id,
                                   onu_persisted_state=self._onu_persisted_state, e=e)
                    # at this point omci is started and/or indications being processed
                    # later indications may have a chance to write this state out again

        except Exception as e:
            self.log.exception("error-processing-inter-adapter-message", e=e)

    def _update_onu_persisted_state(self, uni_id, tp_path):
        # persist the uni and tech profile path for later reconciliation. update only if changed
        update_onu_state = False
        found = False
        for entry in self._onu_persisted_state.get('uni_config', list()):
            if entry.get('uni_id') == uni_id:
                found = True
                if entry.get('tp_path') != tp_path:
                    update_onu_state = True
                    entry['tp_path'] = tp_path

        if not found:
            update_onu_state = True
            uni_tp = {
                'uni_id': uni_id,
                'tp_path': tp_path
            }
            self._onu_persisted_state['uni_config'].append(uni_tp)

        return update_onu_state

    # Called each time there is an onu "up" indication from the olt handler
    @inlineCallbacks
    def create_interface(self, onu_indication):
        self.log.info('create-interface', onu_id=onu_indication.onu_id,
                      serial_number=onu_indication.serial_number)

        # Ignore if onu_indication is received for an already running ONU
        if self._onu_omci_device is not None and self._onu_omci_device.active:
            self.log.warn('received-onu-indication-for-active-onu', onu_indication=onu_indication)
            return

        yield self.core_proxy.device_state_update(self.device_id, oper_status=OperStatus.ACTIVATING,
                                                  connect_status=ConnectStatus.REACHABLE)

        onu_device = yield self.core_proxy.get_device(self.device_id)

        self.log.debug('starting-openomci-statemachine')
        self._subscribe_to_events()
        onu_device.reason = "starting-openomci"
        reactor.callLater(1, self._onu_omci_device.start, onu_device)
        yield self.core_proxy.device_reason_update(self.device_id, onu_device.reason)
        self._heartbeat.enabled = True

    # Called each time there is an onu "down" indication from the olt handler
    @inlineCallbacks
    def update_interface(self, onu_indication):
        self.log.info('update-interface', onu_id=onu_indication.onu_id,
                      serial_number=onu_indication.serial_number)

        if onu_indication.oper_state == 'down' or onu_indication.oper_state == "unreachable":
            self.log.debug('stopping-openomci-statemachine', device_id=self.device_id)
            reactor.callLater(0, self._onu_omci_device.stop)

            self._tp = dict()

            # Let TP download happen again
            for uni_id in self._tp_service_specific_task:
                self._tp_service_specific_task[uni_id].clear()
            for uni_id in self._tech_profile_download_done:
                self._tech_profile_download_done[uni_id].clear()

            yield self.disable_ports(lock_ports=False)
            yield self.core_proxy.device_reason_update(self.device_id, "stopping-openomci")
            yield self.core_proxy.device_state_update(self.device_id, oper_status=OperStatus.DISCOVERED,
                                                      connect_status=ConnectStatus.UNREACHABLE)
        else:
            self.log.debug('not-changing-openomci-statemachine')

    @inlineCallbacks
    def disable(self, device):
        self.log.info('disable', device_id=device.id, serial_number=device.serial_number)
        try:
            yield self.disable_ports(lock_ports=True, device_disabled=True)
            yield self.core_proxy.device_reason_update(self.device_id, "omci-admin-lock")
            yield self.core_proxy.device_state_update(self.device_id, oper_status=OperStatus.UNKNOWN)
        except Exception as e:
            self.log.exception('exception-in-onu-disable', exception=e)

    @inlineCallbacks
    def reenable(self, device):
        self.log.info('reenable', device_id=device.id, serial_number=device.serial_number)
        try:
            yield self.core_proxy.device_state_update(device.id,
                                                      oper_status=OperStatus.ACTIVE,
                                                      connect_status=ConnectStatus.REACHABLE)
            yield self.core_proxy.device_reason_update(self.device_id, 'onu-reenabled')
            yield self.enable_ports()
        except Exception as e:
            self.log.exception('exception-in-onu-reenable', exception=e)

    @inlineCallbacks
    def reboot(self):
        self.log.info('reboot-device')
        device = yield self.core_proxy.get_device(self.device_id)
        if device.connect_status != ConnectStatus.REACHABLE:
            self.log.error("device-unreachable")
            return

        @inlineCallbacks
        def success(_results):
            self.log.info('reboot-success', _results=_results)
            yield self.core_proxy.device_reason_update(self.device_id, 'rebooting')

        def failure(_reason):
            self.log.info('reboot-failure', _reason=_reason)

        self._deferred = self._onu_omci_device.reboot()
        self._deferred.addCallbacks(success, failure)

    @inlineCallbacks
    def disable_ports(self, lock_ports=True, device_disabled=False):
        self.log.info('disable-ports', device_id=self.device_id)

        # TODO: for now only support the first UNI given no requirement for multiple uni yet. Also needed to reduce flow
        #  load on the core
        for port in self.uni_ports:
            if port.mac_bridge_port_num == 1:
                port.operstatus = OperStatus.UNKNOWN
                self.log.info('disable-port', device_id=self.device_id, port=port)
                yield self.core_proxy.port_state_update(self.device_id, Port.ETHERNET_UNI, port.port_number,
                                                        port.operstatus)

        if lock_ports is True:
            self.lock_ports(lock=True, device_disabled=device_disabled)

    @inlineCallbacks
    def enable_ports(self):
        self.log.info('enable-ports', device_id=self.device_id)

        self.lock_ports(lock=False)

        # TODO: for now only support the first UNI given no requirement for multiple uni yet. Also needed to reduce flow
        #  load on the core
        # Given by default all unis are initially active according to omci alarming, we must mimic this.
        for port in self.uni_ports:
            if port.mac_bridge_port_num == 1:
                port.operstatus = OperStatus.ACTIVE
                self.log.info('enable-port', device_id=self.device_id, port=port)
                yield self.core_proxy.port_state_update(self.device_id, Port.ETHERNET_UNI, port.port_number,
                                                        port.operstatus)

    # TODO: Normally we would want any uni ethernet link down or uni ethernet link up alarms to register in the core,
    #  but practically olt provisioning cannot handle the churn of links up, down, then up again typical on startup.
    #
    # Basically the link state sequence:
    # 1) per omci default alarm state, all unis are initially up (no link down alarms received yet)
    # 2) a link state down alarm is received for all uni, given the lock command, and also because most unis have nothing plugged in
    # 3) a link state up alarm is received for the uni plugged in.
    #
    # Given the olt (BAL) has to provision all uni, de-provision all uni, and re-provision one uni in quick succession
    # and cannot (bug?), we have to skip this and leave uni ports as assumed active.  Also all the link state activity
    # would have a ripple effect through the core to the controller as well.  And is it really worth it?
    '''
    @inlineCallbacks
    def port_state_handler(self, _topic, msg):
        self.log.info("port-state-change", _topic=_topic, msg=msg)

        onu_id = msg['onu_id']
        port_no = msg['port_number']
        serial_number = msg['serial_number']
        port_status = msg['port_status']
        uni_port = self.uni_port(int(port_no))

        self.log.debug("port-state-parsed-message", onu_id=onu_id, port_no=port_no, serial_number=serial_number,
                       port_status=port_status)

        if port_status is True:
            uni_port.operstatus = OperStatus.ACTIVE
            self.log.info('link-up', device_id=self.device_id, port=uni_port)
        else:
            uni_port.operstatus = OperStatus.UNKNOWN
            self.log.info('link-down', device_id=self.device_id, port=uni_port)

        yield self.core_proxy.port_state_update(self.device_id, Port.ETHERNET_UNI, uni_port.port_number, uni_port.operstatus)
    '''

    # Called just before openomci state machine is started.  These listen for events from selected state machines,
    # most importantly, mib in sync.  Which ultimately leads to downloading the mib
    def _subscribe_to_events(self):
        self.log.debug('subscribe-to-events')

        bus = self._onu_omci_device.event_bus

        # OMCI MIB Database sync status
        topic = OnuDeviceEntry.event_bus_topic(self.device_id,
                                               OnuDeviceEvents.MibDatabaseSyncEvent)
        self._in_sync_subscription = bus.subscribe(topic, self.in_sync_handler)

        # OMCI Capabilities
        topic = OnuDeviceEntry.event_bus_topic(self.device_id,
                                               OnuDeviceEvents.OmciCapabilitiesEvent)
        self._capabilities_subscription = bus.subscribe(topic, self.capabilties_handler)

        # TODO: these alarms seem to be unreliable depending on the environment
        # Listen for UNI link state alarms and set the oper_state based on that rather than assuming all UNI are up
        # topic = OnuDeviceEntry.event_bus_topic(self.device_id,
        #                                       OnuDeviceEvents.PortEvent)
        # self._port_state_subscription = bus.subscribe(topic, self.port_state_handler)

    # Called when the mib is in sync
    def in_sync_handler(self, _topic, msg):
        self.log.debug('in-sync-handler', _topic=_topic, msg=msg)
        if self._in_sync_subscription is not None:
            try:
                in_sync = msg[IN_SYNC_KEY]

                if in_sync:
                    # Only call this once
                    bus = self._onu_omci_device.event_bus
                    bus.unsubscribe(self._in_sync_subscription)
                    self._in_sync_subscription = None

                    # Start up device_info load
                    self.log.debug('running-mib-sync')
                    reactor.callLater(0, self._mib_in_sync)

            except Exception as e:
                self.log.exception('in-sync', e=e)

    def capabilties_handler(self, _topic, _msg):
        self.log.debug('capabilities-handler', _topic=_topic, msg=_msg)
        if self._capabilities_subscription is not None:
            self.log.debug('capabilities-handler-done')

    # Mib is in sync, we can now query what we learned and actually start pushing ME (download) to the ONU.
    @inlineCallbacks
    def _mib_in_sync(self):
        self.log.debug('mib-in-sync')
        device = yield self.core_proxy.get_device(self.device_id)

        # only notify core if this is a new device.  otherwise do not have reconcile generating
        # a lot of needless message churn
        if not self._reconciling:
            yield self.core_proxy.device_reason_update(self.device_id, 'discovery-mibsync-complete')

        if self._dev_info_loaded:
            self.log.debug('device-info-already-loaded')
        else:
            # new onu or adapter was restarted.  fill up our local data
            yield self._load_device_data(device)

        if self._check_mib_downloaded():
            self.log.debug('mib-already-downloaded')
            if not self._reconciling:
                yield self.core_proxy.device_state_update(device.id,
                                                          oper_status=OperStatus.ACTIVE,
                                                          connect_status=ConnectStatus.REACHABLE)
                yield self.enable_ports()
        else:
            self._download_mib(device)

        if self._reconciling:
            yield self._restore_tech_profile()
            self._start_monitoring()
            self._reconciling = False
            self.log.debug('reconcile-finished')

    def _download_mib(self, device):
        self.log.debug('downloading-initial-mib-configuration')

        @inlineCallbacks
        def success(_results):
            self.log.debug('mib-download-success', _results=_results)
            yield self.core_proxy.device_state_update(device.id,
                                                      oper_status=OperStatus.ACTIVE,
                                                      connect_status=ConnectStatus.REACHABLE)
            yield self.core_proxy.device_reason_update(self.device_id, 'initial-mib-downloaded')
            self._mib_download_task = None
            yield self.enable_ports()
            yield self.onu_active_event()
            self._start_monitoring()

        @inlineCallbacks
        def failure(_reason):
            self.log.warn('mib-download-failure-retrying', _reason=_reason)
            retry = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
            reactor.callLater(retry, self._mib_in_sync)
            yield self.core_proxy.device_reason_update(self.device_id, 'initial-mib-download-failure-retrying')

        # start by locking all the unis till mib sync and initial mib is downloaded
        # this way we can capture the port down/up events when we are ready
        self.lock_ports(lock=True)

        # Download an initial mib that creates simple bridge that can pass EAP.  On success (above) finally set
        # the device to active/reachable.   This then opens up the handler to openflow pushes from outside
        self._mib_download_task = BrcmMibDownloadTask(self.omci_agent, self)
        self._deferred = self._onu_omci_device.task_runner.queue_task(self._mib_download_task)
        self._deferred.addCallbacks(success, failure)

    def _start_monitoring(self):
        self.log.debug('starting-monitoring')

        # Start collecting stats from the device after a brief pause
        if not self._pm_metrics_started:
            self._pm_metrics_started = True
            pmstart = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
            reactor.callLater(pmstart, self._pm_metrics.start_collector)

        # Start test requests after a brief pause
        if not self._test_request_started:
            self._test_request_started = True
            tststart = _STARTUP_RETRY_WAIT * (random.randint(1, 5))
            reactor.callLater(tststart, self._test_request.start_collector)

    def _check_mib_downloaded(self):
        self.log.debug('checking-mib-downloaded')
        results = False

        mac_bridges = self.onu_omci_device.query_mib(MacBridgeServiceProfile.class_id)
        self.log.debug('mac-bridges', mac_bridges=mac_bridges)

        for k, v in mac_bridges.items():
            if not isinstance(v, dict):
                continue
            # found at least one mac bridge, good enough to say its done, break out
            self.log.debug('found-mac-bridge-mib-download-has-been-done', omci_key=k, omci_value=v)
            results = True
            break

        return results

    @inlineCallbacks
    def _load_device_data(self, device):
        self.log.debug('loading-device-data-from-mib', device_id=device.id)

        omci_dev = self._onu_omci_device
        config = omci_dev.configuration

        try:
            # sort the lists so we get consistent port ordering.
            ani_list = sorted(config.ani_g_entities) if config.ani_g_entities else []
            uni_list = sorted(config.uni_g_entities) if config.uni_g_entities else []
            pptp_list = sorted(config.pptp_entities) if config.pptp_entities else []
            veip_list = sorted(config.veip_entities) if config.veip_entities else []

            if ani_list is None or (pptp_list is None and veip_list is None):
                yield self.core_proxy.device_reason_update(self.device_id, 'onu-missing-required-elements')
                raise Exception("onu-missing-required-elements")

            # Currently logging the ani, pptp, veip, and uni for information purposes.
            # Actually act on the veip/pptp as its ME is the most correct one to use in later tasks.
            # And in some ONU the UNI-G list is incomplete or incorrect...
            for entity_id in ani_list:
                ani_value = config.ani_g_entities[entity_id]
                self.log.debug("discovered-ani", entity_id=entity_id, value=ani_value)

            for entity_id in uni_list:
                uni_value = config.uni_g_entities[entity_id]
                self.log.debug("discovered-uni", entity_id=entity_id, value=uni_value)

            uni_entities = OrderedDict()
            for entity_id in pptp_list:
                pptp_value = config.pptp_entities[entity_id]
                self.log.debug("discovered-pptp", entity_id=entity_id, value=pptp_value)
                uni_entities[entity_id] = UniType.PPTP

            for entity_id in veip_list:
                veip_value = config.veip_entities[entity_id]
                self.log.debug("discovered-veip", entity_id=entity_id, value=veip_value)
                uni_entities[entity_id] = UniType.VEIP

            uni_id = 0
            for entity_id, uni_type in uni_entities.items():
                yield self._add_uni_port(device, entity_id, uni_id, uni_type)
                uni_id += 1

            if self._unis:
                self._dev_info_loaded = True
            else:
                yield self.core_proxy.device_reason_update(self.device_id, 'no-usable-unis')
                raise Exception("no-usable-unis")

        except Exception as e:
            self.log.exception('device-info-load', e=e)
            self._deferred = reactor.callLater(_STARTUP_RETRY_WAIT, self._mib_in_sync)

    @inlineCallbacks
    def _add_uni_port(self, device, entity_id, uni_id, uni_type=UniType.PPTP):
        self.log.debug('add-uni-port', entity_id=entity_id, uni_id=uni_id)

        intf_id = self._onu_persisted_state.get('intf_id')
        onu_id = self._onu_persisted_state.get('onu_id')
        uni_no = self.mk_uni_port_num(intf_id, onu_id, uni_id)

        # TODO: Some or parts of this likely need to move to UniPort. especially the format stuff
        uni_name = "uni-{}".format(uni_no)

        mac_bridge_port_num = uni_id + 1  # TODO +1 is only to test non-zero index

        self.log.debug('uni-port-inputs', uni_no=uni_no, uni_id=uni_id, uni_name=uni_name, uni_type=uni_type,
                       entity_id=entity_id, mac_bridge_port_num=mac_bridge_port_num, serial_number=device.serial_number)

        uni_port = UniPort.create(self, uni_name, uni_id, uni_no, uni_name, uni_type)
        uni_port.entity_id = entity_id
        uni_port.enabled = True
        uni_port.mac_bridge_port_num = mac_bridge_port_num

        self.log.debug("created-uni-port", uni=uni_port)

        if not self._reconciling:
            yield self.core_proxy.port_created(device.id, uni_port.get_port())

        self._unis[uni_port.port_number] = uni_port

        self._onu_omci_device.alarm_synchronizer.set_alarm_params(onu_id=onu_id,
                                                                  uni_ports=self.uni_ports,
                                                                  serial_number=device.serial_number)

    @inlineCallbacks
    def _restore_tech_profile(self):
        self.log.debug("reconcile-restoring-tech-profile-tcont-gem-config")

        # for every uni that has tech profile config reload all its tcont/alloc_id and gem from the tp path
        for entry in self._onu_persisted_state.get('uni_config', list()):
            uni_id = entry.get('uni_id')
            tp_path = entry.get('tp_path')
            if tp_path:
                tpstored = yield self.tp_kv_client.get(tp_path)
                tpstring = tpstored.decode('ascii')
                tp = json.loads(tpstring)

                self.log.debug("restoring-tp-instance", tp=tp)

                # re-run tech profile config that stores gem and tconts in the self._pon object
                # this does not actually re-run the omci, just rebuilds our local data store
                self._do_tech_profile_configuration(uni_id, tp)

                tp_id = self.extract_tp_id_from_path(tp_path)

                # rebuild cache dicts so tp updates and deletes dont get KeyErrors
                if uni_id not in self._tp_service_specific_task:
                    self._tp_service_specific_task[uni_id] = dict()

                if uni_id not in self._tech_profile_download_done:
                    self._tech_profile_download_done[uni_id] = dict()

                if tp_id not in self._tech_profile_download_done[uni_id]:
                    self._tech_profile_download_done[uni_id][tp_id] = True
            else:
                self.log.debug("no-assigned-tp-instance", uni_id=uni_id)

        # for every loaded tcont from tp check the mib database for its entity_id
        # needed for later tp deletes/adds
        tcont_idents = self.onu_omci_device.query_mib(Tcont.class_id)
        self.log.debug('tcont-idents', tcont_idents=tcont_idents)

        for k, v in tcont_idents.items():
            if not isinstance(v, dict):
                continue
            alloc_check = v.get('attributes', {}).get('alloc_id', 0)
            tcont = self._pon.tconts.get(alloc_check)
            if tcont:
                tcont.entity_id = k
                self.log.debug('reassigning-tcont-entity-id', entity_id=tcont.entity_id,
                               alloc_id=tcont.alloc_id)

    # TODO NEW CORE: Figure out how to gain this knowledge from the olt.  for now cheat terribly.
    def mk_uni_port_num(self, intf_id, onu_id, uni_id):
        MAX_PONS_PER_OLT = 256
        MAX_ONUS_PER_PON = 256
        MAX_UNIS_PER_ONU = 16

        assert intf_id < MAX_PONS_PER_OLT
        assert onu_id < MAX_ONUS_PER_PON
        assert uni_id < MAX_UNIS_PER_ONU
        return intf_id << 12 | onu_id << 4 | uni_id

    @inlineCallbacks
    def onu_active_event(self):
        self.log.debug('onu-active-event')
        try:
            # TODO: this is expensive for just getting the olt serial number.  replace with direct api call
            parent_device = yield self.core_proxy.get_device(self.parent_id)
            olt_serial_number = parent_device.serial_number
            raised_ts = arrow.utcnow().timestamp

            intf_id = self._onu_persisted_state.get('intf_id')
            onu_id = self._onu_persisted_state.get('onu_id')
            onu_serial = self._onu_persisted_state.get('serial_number')

            self.log.debug("onu-indication-context-data",
                           pon_id=intf_id,
                           onu_id=onu_id,
                           registration_id=self.device_id,
                           device_id=self.device_id,
                           onu_serial_number=onu_serial,
                           olt_serial_number=olt_serial_number,
                           raised_ts=raised_ts)

            self.log.debug("Trying-to-raise-onu-active-event")
            OnuActiveEvent(self.events, self.device_id,
                           intf_id,
                           onu_serial,
                           str(self.device_id),
                           olt_serial_number, raised_ts,
                           onu_id=onu_id).send(True)
        except Exception as active_event_error:
            self.log.exception('onu-activated-event-error',
                               errmsg=active_event_error)

    @inlineCallbacks
    def onu_disabled_event(self):
        self.log.debug('onu-disabled-event')
        try:
            device = yield self.core_proxy.get_device(self.device_id)
            parent_device = yield self.core_proxy.get_device(self.parent_id)
            olt_serial_number = parent_device.serial_number
            raised_ts = arrow.utcnow().timestamp
            intf_id = self._onu_persisted_state.get('intf_id')
            onu_id = self._onu_persisted_state.get('onu_id')
            onu_serial = self._onu_persisted_state.get('serial_number')

            self.log.debug("onu-indication-context-data",
                           pon_id=intf_id,
                           onu_id=onu_id,
                           registration_id=self.device_id,
                           device_id=self.device_id,
                           onu_serial_number=onu_serial,
                           olt_serial_number=olt_serial_number,
                           raised_ts=raised_ts)

            self.log.debug("Trying-to-raise-onu-disabled-event")
            OnuDisabledEvent(self.events, self.device_id,
                             intf_id,
                             device.serial_number,
                             str(self.device_id),
                             olt_serial_number, raised_ts,
                             onu_id=onu_id).send(True)
        except Exception as disable_event_error:
            self.log.exception('onu-disabled-event-error',
                               errmsg=disable_event_error)

    @inlineCallbacks
    def onu_deleted_event(self):
        self.log.debug('onu-deleted-event')
        try:
            device = yield self.core_proxy.get_device(self.device_id)
            parent_device = yield self.core_proxy.get_device(self.parent_id)
            olt_serial_number = parent_device.serial_number
            raised_ts = arrow.utcnow().timestamp
            intf_id = self._onu_persisted_state.get('intf_id')
            onu_id = self._onu_persisted_state.get('onu_id')
            serial_number = self._onu_persisted_state.get('serial_number')

            self.log.debug("onu-deleted-event-context-data",
                           pon_id=intf_id,
                           onu_id=onu_id,
                           registration_id=self.device_id,
                           device_id=self.device_id,
                           onu_serial_number=serial_number,
                           olt_serial_number=olt_serial_number,
                           raised_ts=raised_ts)

            OnuDeletedEvent(self.events, self.device_id,
                             intf_id,
                             serial_number,
                             str(self.device_id),
                             olt_serial_number, raised_ts,
                             onu_id=onu_id).send(True)
        except Exception as deleted_event_error:
            self.log.exception('onu-deleted-event-error',
                               errmsg=deleted_event_error)

    def lock_ports(self, lock=True, device_disabled=False):

        def success(response):
            self.log.debug('set-onu-ports-state', lock=lock, response=response)
            if device_disabled:
                self.onu_disabled_event()

        def failure(response):
            self.log.error('cannot-set-onu-ports-state', lock=lock, response=response)

        task = BrcmUniLockTask(self.omci_agent, self.device_id, lock=lock)
        self._deferred = self._onu_omci_device.task_runner.queue_task(task)
        self._deferred.addCallbacks(success, failure)

    def extract_tp_id_from_path(self, tp_path):
        # tp_path is of the format  <technology>/<table_id>/<uni_port_name>
        tp_id = int(tp_path.split(_PATH_SEPERATOR)[1])
        return tp_id

    def start_omci_test_action(self, device, uuid):
        """

        :param device:
        :return:
        """
        # Code to Run OMCI Test Action
        self.log.info('Omci-test-action-request-On', request=device.id)
        kwargs_omci_test_action = {
            OmciTestRequest.DEFAULT_FREQUENCY_KEY:
                OmciTestRequest.DEFAULT_COLLECTION_FREQUENCY
        }
        serial_number = device.serial_number
        if device.connect_status != ConnectStatus.REACHABLE or device.admin_state != AdminState.ENABLED:
            return (TestResponse(result=TestResponse.FAILURE))
        test_request = OmciTestRequest(self.core_proxy,
                                       self.omci_agent, self.device_id, AniG,
                                       serial_number,
                                       self.logical_device_id, exclusive=False,
                                       uuid=uuid,
                                       **kwargs_omci_test_action)
        test_request.perform_test_omci()
        return (TestResponse(result=TestResponse.SUCCESS))
