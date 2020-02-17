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
from twisted.internet.defer import DeferredQueue, inlineCallbacks

from heartbeat import HeartBeat
from pyvoltha.adapters.extensions.events.device_events.onu.onu_active_event import OnuActiveEvent
from pyvoltha.adapters.extensions.events.kpi.onu.onu_pm_metrics import OnuPmMetrics
from pyvoltha.adapters.extensions.events.kpi.onu.onu_omci_pm import OnuOmciPmMetrics
from pyvoltha.adapters.extensions.events.adapter_events import AdapterEvents

import pyvoltha.common.openflow.utils as fd
from pyvoltha.common.utils.registry import registry
from pyvoltha.adapters.common.frameio.frameio import hexify
from pyvoltha.common.utils.nethelpers import mac_str_to_tuple
from pyvoltha.common.config.config_backend import ConsulStore
from pyvoltha.common.config.config_backend import EtcdStore
from voltha_protos.logical_device_pb2 import LogicalPort
from voltha_protos.common_pb2 import OperStatus, ConnectStatus, AdminState
from voltha_protos.device_pb2 import Port
from voltha_protos.openflow_13_pb2 import OFPXMC_OPENFLOW_BASIC, ofp_port, OFPPS_LIVE,OFPPS_LINK_DOWN,\
    OFPPF_FIBER, OFPPF_1GB_FD
from voltha_protos.inter_container_pb2 import InterAdapterMessageType, \
    InterAdapterOmciMessage, PortCapability, InterAdapterTechProfileDownloadMessage, InterAdapterDeleteGemPortMessage, \
    InterAdapterDeleteTcontMessage
from voltha_protos.openolt_pb2 import OnuIndication
from pyvoltha.adapters.extensions.omci.onu_configuration import OMCCVersion
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
from uni_port import UniPort, UniType
from pyvoltha.common.tech_profile.tech_profile import TechProfile
from pyvoltha.adapters.extensions.omci.tasks.omci_test_request import OmciTestRequest
from pyvoltha.adapters.extensions.omci.omci_entities import AniG
from pyvoltha.adapters.extensions.omci.omci_defs import EntityOperations, ReasonCodes

OP = EntityOperations
RC = ReasonCodes

_STARTUP_RETRY_WAIT = 10


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
        self._omcc_version = OMCCVersion.Unknown
        self._total_tcont_count = 0  # From ANI-G ME
        self._qos_flexibility = 0  # From ONT2_G ME

        self._onu_indication = None
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
        # Stores information related to queued vlan filter tasks
        # Dictionary with key being uni_id and value being device,uni port ,uni id and vlan id

        self._queued_vlan_filter_task = dict()

        # Initialize KV store client
        self.args = registry('main').get_args()
        if self.args.backend == 'etcd':
            host, port = self.args.etcd.split(':', 1)
            self.kv_client = EtcdStore(host, port,
                                       TechProfile.KV_STORE_TECH_PROFILE_PATH_PREFIX)
        elif self.args.backend == 'consul':
            host, port = self.args.consul.split(':', 1)
            self.kv_client = ConsulStore(host, port,
                                         TechProfile.KV_STORE_TECH_PROFILE_PATH_PREFIX)
        else:
            self.log.error('invalid-backend')
            raise Exception("invalid-backend-for-kv-store")

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

            yield self.core_proxy.device_update(device)
            self.log.debug('device-updated', device_id=device.id, serial_number=device.serial_number)

            yield self._init_pon_state()

            self.log.debug('pon state initialized', device_id=device.id, serial_number=device.serial_number)
            ############################################################################
            # Setup Alarm handler
            self.events = AdapterEvents(self.core_proxy, device.id, self.logical_device_id,
                                        device.serial_number)
            ############################################################################
            # Setup PM configuration for this device
            # Pass in ONU specific options
            kwargs = {
                OnuPmMetrics.DEFAULT_FREQUENCY_KEY: OnuPmMetrics.DEFAULT_ONU_COLLECTION_FREQUENCY,
                'heartbeat': self.heartbeat,
                OnuOmciPmMetrics.OMCI_DEV_KEY: self._onu_omci_device
            }
            self.log.debug('create-pm-metrics', device_id=device.id, serial_number=device.serial_number)
            self._pm_metrics = OnuPmMetrics(self.events, self.core_proxy, self.device_id,
                                           self.logical_device_id, device.serial_number,
                                           grouped=True, freq_override=False, **kwargs)
            pm_config = self._pm_metrics.make_proto()
            self._onu_omci_device.set_pm_config(self._pm_metrics.omci_pm.openomci_interval_pm)
            self.log.info("initial-pm-config", device_id=device.id, serial_number=device.serial_number)
            yield self.core_proxy.device_pm_config_update(pm_config, init=True)

            # Note, ONU ID and UNI intf set in add_uni_port method
            self._onu_omci_device.alarm_synchronizer.set_alarm_params(mgr=self.events,
                                                                      ani_ports=[self._pon])

            # Code to Run OMCI Test Action
            kwargs_omci_test_action = {
                OmciTestRequest.DEFAULT_FREQUENCY_KEY:
                    OmciTestRequest.DEFAULT_COLLECTION_FREQUENCY
            }
            serial_number = device.serial_number
            self._test_request = OmciTestRequest(self.core_proxy,
                                           self.omci_agent, self.device_id,
                                           AniG, serial_number,
                                           self.logical_device_id,
                                           exclusive=False,
                                           **kwargs_omci_test_action)

            self.enabled = True
        else:
            self.log.info('onu-already-activated')

    # Called once when the adapter needs to re-create device.  usually on vcore restart
    @inlineCallbacks
    def reconcile(self, device):
        self.log.debug('reconcile-device', device_id=device.id, serial_number=device.serial_number)

        # first we verify that we got parent reference and proxy info
        assert device.parent_id
        assert device.proxy_address.device_id

        self.proxy_address = device.proxy_address
        self.parent_id = device.parent_id
        self._pon_port_number = device.parent_port_no

        if self.enabled is not True:
            self.log.info('reconciling-broadcom-onu-device')
            self.logical_device_id = self.device_id
            self._init_pon_state()

            # need to restart state machines on vcore restart.  there is no indication to do it for us.
            self._onu_omci_device.start()
            yield self.core_proxy.device_reason_update(self.device_id, "restarting-openomci")

            # TODO: this is probably a bit heavy handed
            # Force a reboot for now.  We need indications to reflow to reassign tconts and gems given vcore went away
            # This may not be necessary when mib resync actually works
            reactor.callLater(1, self.reboot)

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

    def delete(self, device):
        self.log.info('delete-onu', device_id=device.id, serial_number=device.serial_number)

        self._deferred.cancel()
        self._test_request.stop_collector()
        self._pm_metrics.stop_collector()
        self.log.debug('removing-openomci-statemachine')
        self.omci_agent.remove_device(device.id, cleanup=True)

    def _create_tconts(self, uni_id, us_scheduler):
        alloc_id = us_scheduler['alloc_id']
        q_sched_policy = us_scheduler['q_sched_policy']
        self.log.debug('create-tcont', us_scheduler=us_scheduler)

        tcontdict = dict()
        tcontdict['alloc-id'] = alloc_id
        tcontdict['q_sched_policy'] = q_sched_policy
        tcontdict['uni_id'] = uni_id

        tcont = OnuTCont.create(self, tcont=tcontdict)

        self._pon.add_tcont(tcont)

        self.log.debug('pon-add-tcont', tcont=tcont)

    # Called when there is an olt up indication, providing the gem port id chosen by the olt handler
    def _create_gemports(self, uni_id, gem_ports, alloc_id_ref, direction):
        self.log.debug('create-gemport',
                       gem_ports=gem_ports, direction=direction)
        new_gem_ports = []
        for gem_port in gem_ports:
            gemdict = dict()
            gemdict['gemport_id'] = gem_port['gemport_id']
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
            new_gem_ports.append(gem_port)

            self._pon.add_gem_port(gem_port)

            self.log.debug('pon-add-gemport', gem_port=gem_port)

        return new_gem_ports

    def _execute_queued_vlan_filter_tasks(self, uni_id):
        # During OLT Reboots, ONU Reboots, ONU Disable/Enable, it is seen that vlan_filter
        # task is scheduled even before tp task. So we queue vlan-filter task if tp_task
        # or initial-mib-download is not done. Once the tp_task is completed, we execute
        # such queued vlan-filter tasks
        try:
            if uni_id in self._queued_vlan_filter_task:
                self.log.info("executing-queued-vlan-filter-task",
                              uni_id=uni_id)
                filter_info = self._queued_vlan_filter_task[uni_id]
                reactor.callLater(0, self._add_vlan_filter_task, filter_info.get("device"),
                                  uni_id, filter_info.get("uni_port"), filter_info.get("set_vlan_vid"),
                                  filter_info.get("tp_id"))
                # Now remove the entry from the dictionary
                self._queued_vlan_filter_task[uni_id].clear()
                self.log.debug("executed-queued-vlan-filter-task",
                               uni_id=uni_id)
        except Exception as e:
            self.log.error("vlan-filter-configuration-failed", uni_id=uni_id, error=e)

    def _do_tech_profile_configuration(self, uni_id, tp):
        us_scheduler = tp['us_scheduler']
        alloc_id = us_scheduler['alloc_id']
        self._create_tconts(uni_id, us_scheduler)
        upstream_gem_port_attribute_list = tp['upstream_gem_port_attribute_list']
        self._create_gemports(uni_id, upstream_gem_port_attribute_list, alloc_id, "UPSTREAM")
        downstream_gem_port_attribute_list = tp['downstream_gem_port_attribute_list']
        self._create_gemports(uni_id, downstream_gem_port_attribute_list, alloc_id, "DOWNSTREAM")

    def load_and_configure_tech_profile(self, uni_id, tp_path):
        self.log.debug("loading-tech-profile-configuration", uni_id=uni_id, tp_path=tp_path)

        if uni_id not in self._tp_service_specific_task:
            self._tp_service_specific_task[uni_id] = dict()

        if uni_id not in self._tech_profile_download_done:
            self._tech_profile_download_done[uni_id] = dict()

        if tp_path not in self._tech_profile_download_done[uni_id]:
            self._tech_profile_download_done[uni_id][tp_path] = False

        if not self._tech_profile_download_done[uni_id][tp_path]:
            try:
                if tp_path in self._tp_service_specific_task[uni_id]:
                    self.log.info("tech-profile-config-already-in-progress",
                                  tp_path=tp_path)
                    return

                tpstored = self.kv_client[tp_path]
                tpstring = tpstored.decode('ascii')
                tp = json.loads(tpstring)

                self.log.debug("tp-instance", tp=tp)
                self._do_tech_profile_configuration(uni_id, tp)

                @inlineCallbacks
                def success(_results):
                    self.log.info("tech-profile-config-done-successfully")
                    if tp_path in self._tp_service_specific_task[uni_id]:
                        del self._tp_service_specific_task[uni_id][tp_path]
                    self._tech_profile_download_done[uni_id][tp_path] = True
                    # Now execute any vlan filter tasks that were queued for later
                    self._execute_queued_vlan_filter_tasks(uni_id)
                    yield self.core_proxy.device_reason_update(self.device_id, 'tech-profile-config-download-success')

                @inlineCallbacks
                def failure(_reason):
                    self.log.warn('tech-profile-config-failure-retrying',
                                  _reason=_reason)
                    if tp_path in self._tp_service_specific_task[uni_id]:
                        del self._tp_service_specific_task[uni_id][tp_path]
                    retry = _STARTUP_RETRY_WAIT * (random.randint(1,5))
                    reactor.callLater(retry, self.load_and_configure_tech_profile,
                                      uni_id, tp_path)
                    yield self.core_proxy.device_reason_update(self.device_id,
                                                               'tech-profile-config-download-failure-retrying')

                self.log.info('downloading-tech-profile-configuration')
                # Extract the current set of TCONT and GEM Ports from the Handler's pon_port that are
                # relevant to this task's UNI. It won't change. But, the underlying pon_port may change
                # due to additional tasks on different UNIs. So, it we cannot use the pon_port after
                # this initializer
                tconts = []
                for tcont in list(self.pon_port.tconts.values()):
                    if tcont.uni_id is not None and tcont.uni_id != uni_id:
                        continue
                    tconts.append(tcont)

                gem_ports = []
                for gem_port in list(self.pon_port.gem_ports.values()):
                    if gem_port.uni_id is not None and gem_port.uni_id != uni_id:
                        continue
                    gem_ports.append(gem_port)

                self.log.debug("tconts-gems-to-install", tconts=tconts, gem_ports=gem_ports)

                self._tp_service_specific_task[uni_id][tp_path] = \
                    BrcmTpSetupTask(self.omci_agent, self, uni_id, tconts, gem_ports, int(tp_path.split("/")[1]))
                self._deferred = \
                    self._onu_omci_device.task_runner.queue_task(self._tp_service_specific_task[uni_id][tp_path])
                self._deferred.addCallbacks(success, failure)

            except Exception as e:
                self.log.exception("error-loading-tech-profile", e=e)
        else:
            self.log.info("tech-profile-config-already-done")
            # Could be a case where TP exists but new gem-ports are getting added dynamically
            tpstored = self.kv_client[tp_path]
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

                    retry = _STARTUP_RETRY_WAIT * (random.randint(1,5))
                    reactor.callLater(retry, self.load_and_configure_tech_profile,
                                      uni_id, tp_path)

                self._tp_service_specific_task[uni_id][tp_path] = \
                    BrcmTpSetupTask(self.omci_agent, self, uni_id, [], new_gems, int(tp_path.split("/")[1]))
                self._deferred = \
                    self._onu_omci_device.task_runner.queue_task(self._tp_service_specific_task[uni_id][tp_path])
                self._deferred.addCallbacks(success, failure)

    def delete_tech_profile(self, uni_id, tp_path, alloc_id=None, gem_port_id=None):
        try:
            if not uni_id in self._tech_profile_download_done:
                self.log.warn("tp-key-is-not-present", uni_id=uni_id)
                return

            if not tp_path in self._tech_profile_download_done[uni_id]:
                self.log.warn("tp-path-is-not-present", tp_path=tp_path)
                return

            if self._tech_profile_download_done[uni_id][tp_path] is not True:
                self.log.error("tp-download-is-not-done-in-order-to-process-tp-delete")
                return

            if alloc_id is None and gem_port_id is None:
                self.log.error("alloc-id-and-gem-port-id-are-none")
                return

            # Extract the current set of TCONT and GEM Ports from the Handler's pon_port that are
            # relevant to this task's UNI. It won't change. But, the underlying pon_port may change
            # due to additional tasks on different UNIs. So, it we cannot use the pon_port affter
            # this initializer
            tcont = None
            self.log.debug("tconts", tconts=list(self.pon_port.tconts.values()))
            for tc in list(self.pon_port.tconts.values()):
                if tc.alloc_id == alloc_id:
                    tcont = tc
                    self.pon_port.remove_tcont(tc.alloc_id, False)

            gem_port = None
            self.log.debug("gem-ports", gem_ports=list(self.pon_port.gem_ports.values()))
            for gp in list(self.pon_port.gem_ports.values()):
                if gp.gem_id == gem_port_id:
                    gem_port = gp
                    self.pon_port.remove_gem_id(gp.gem_id, gp.direction, False)

            # tp_path is of the format  <technology>/<table_id>/<uni_port_name>
            # We need the TP Table ID
            tp_table_id = int(tp_path.split("/")[1])

            @inlineCallbacks
            def success(_results):
                if gem_port_id:
                    self.log.info("gem-port-delete-done-successfully")
                if alloc_id:
                    self.log.info("tcont-delete-done-successfully")
                    # The deletion of TCONT marks the complete deletion of tech-profile
                    try:
                        del self._tech_profile_download_done[uni_id][tp_path]
                        del self._tp_service_specific_task[uni_id][tp_path]
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

    def update_pm_config(self, device, pm_config):
        # TODO: This has not been tested
        self.log.info('update_pm_config', pm_config=pm_config)
        self._pm_metrics.update(pm_config)

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
            _tunnel_id = None

            try:
                write_metadata = fd.get_write_metadata(flow)
                if write_metadata is None:
                    self.log.error("do-not-process-flow-without-write-metadata")
                    return

                # extract tp id from flow
                tp_id = (write_metadata >> 32) & 0xFFFF
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
                        else:
                            self.log.error('unsupported-action-set-field-type',
                                           field_type=_field.type)
                    else:
                        self.log.error('unsupported-action-type',
                                       action_type=action.type, in_port=_in_port)

                # OMCI set vlan task can only filter and set on vlan header attributes.  Any other openflow
                # supported match and action criteria cannot be handled by omci and must be ignored.
                if _set_vlan_vid is None or _set_vlan_vid == 0:
                    self.log.warn('ignoring-flow-that-does-not-set-vlanid')
                else:
                    self.log.info('set-vlanid', uni_id=uni_id, uni_port=uni_port, set_vlan_vid=_set_vlan_vid, tp_id=tp_id)
                    self._add_vlan_filter_task(device, uni_id, uni_port, _set_vlan_vid, tp_id)
            except Exception as e:
                self.log.exception('failed-to-install-flow', e=e, flow=flow)

    def _add_vlan_filter_task(self, device, uni_id, uni_port, _set_vlan_vid, tp_id):
        assert uni_port is not None
        if uni_id in self._tech_profile_download_done and self._tech_profile_download_done[uni_id] != {}:
            @inlineCallbacks
            def success(_results):
                self.log.info('vlan-tagging-success', uni_port=uni_port, vlan=_set_vlan_vid, tp_id=tp_id)
                self._vlan_filter_task = None
                yield self.core_proxy.device_reason_update(self.device_id, 'omci-flows-pushed')

            @inlineCallbacks
            def failure(_reason):
                self.log.warn('vlan-tagging-failure', uni_port=uni_port, vlan=_set_vlan_vid, tp_id=tp_id)
                retry = _STARTUP_RETRY_WAIT * (random.randint(1,5))
                reactor.callLater(retry,
                                  self._add_vlan_filter_task, device, uni_port.port_number,
                                  uni_port, _set_vlan_vid, tp_id)
                yield self.core_proxy.device_reason_update(self.device_id, 'omci-flows-failed-retrying')

            self.log.info('setting-vlan-tag')
            self._vlan_filter_task = BrcmVlanFilterTask(self.omci_agent, self, uni_port, _set_vlan_vid, tp_id)
            self._deferred = self._onu_omci_device.task_runner.queue_task(self._vlan_filter_task)
            self._deferred.addCallbacks(success, failure)
        else:
            self.log.info('tp-service-specific-task-not-done-adding-request-to-local-cache',
                          uni_id=uni_id)
            self._queued_vlan_filter_task[uni_id] = {"device": device,
                                                     "uni_id": uni_id,
                                                     "uni_port": uni_port,
                                                     "set_vlan_vid": _set_vlan_vid,
                                                     "tp_id": tp_id}

    def process_inter_adapter_message(self, request):
        self.log.debug('process-inter-adapter-message', type=request.header.type, from_topic=request.header.from_topic,
                       to_topic=request.header.to_topic, to_device_id=request.header.to_device_id)
        try:
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

                if onu_indication.oper_state == "up":
                    self.create_interface(onu_indication)
                elif onu_indication.oper_state == "down" or onu_indication.oper_state == "unreachable":
                    self.update_interface(onu_indication)
                else:
                    self.log.error("unknown-onu-indication", onu_id=onu_indication.onu_id,
                                   serial_number=onu_indication.serial_number)

            elif request.header.type == InterAdapterMessageType.TECH_PROFILE_DOWNLOAD_REQUEST:
                tech_msg = InterAdapterTechProfileDownloadMessage()
                request.body.Unpack(tech_msg)
                self.log.debug('inter-adapter-recv-tech-profile', tech_msg=tech_msg)

                self.load_and_configure_tech_profile(tech_msg.uni_id, tech_msg.path)

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

                self.delete_tech_profile(uni_id=del_tcont_msg.uni_id,
                                         alloc_id=del_tcont_msg.alloc_id,
                                         tp_path=del_tcont_msg.tp_path)
            else:
                self.log.error("inter-adapter-unhandled-type", request=request)

        except Exception as e:
            self.log.exception("error-processing-inter-adapter-message", e=e)

    # Called each time there is an onu "up" indication from the olt handler
    @inlineCallbacks
    def create_interface(self, onu_indication):
        self.log.info('create-interface', onu_id=onu_indication.onu_id,
                       serial_number=onu_indication.serial_number)

        # Ignore if onu_indication is received for an already running ONU
        if self._onu_omci_device is not None and self._onu_omci_device.active:
            self.log.warn('received-onu-indication-for-active-onu', onu_indication=onu_indication)
            return

        self._onu_indication = onu_indication

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
            self.log.debug('stopping-openomci-statemachine')
            reactor.callLater(0, self._onu_omci_device.stop)

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
            yield self.disable_ports(lock_ports=True)
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
    def disable_ports(self, lock_ports=True):
        self.log.info('disable-ports', device_id=self.device_id)

        # TODO: for now only support the first UNI given no requirement for multiple uni yet. Also needed to reduce flow
        #  load on the core
        for port in self.uni_ports:
            if port.mac_bridge_port_num == 1:
                port.operstatus = OperStatus.UNKNOWN
                self.log.info('disable-port', device_id=self.device_id, port=port)
                yield self.core_proxy.port_state_update(self.device_id, Port.ETHERNET_UNI, port.port_number, port.operstatus)

        if lock_ports is True:
            self.lock_ports(lock=True)

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
                yield self.core_proxy.port_state_update(self.device_id, Port.ETHERNET_UNI, port.port_number, port.operstatus)


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
        #topic = OnuDeviceEntry.event_bus_topic(self.device_id,
        #                                       OnuDeviceEvents.PortEvent)
        #self._port_state_subscription = bus.subscribe(topic, self.port_state_handler)

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
    # Currently uses a basic mib download task that create a bridge with a single gem port and uni, only allowing EAP
    # Implement your own MibDownloadTask if you wish to setup something different by default
    @inlineCallbacks
    def _mib_in_sync(self):
        self.log.debug('mib-in-sync')

        omci = self._onu_omci_device
        in_sync = omci.mib_db_in_sync

        device = yield self.core_proxy.get_device(self.device_id)
        yield self.core_proxy.device_reason_update(self.device_id, 'discovery-mibsync-complete')

        if not self._dev_info_loaded:
            self.log.info('loading-device-data-from-mib', in_sync=in_sync, already_loaded=self._dev_info_loaded)

            omci_dev = self._onu_omci_device
            config = omci_dev.configuration

            try:

                # sort the lists so we get consistent port ordering.
                ani_list = sorted(config.ani_g_entities) if config.ani_g_entities else []
                uni_list = sorted(config.uni_g_entities) if config.uni_g_entities else []
                pptp_list = sorted(config.pptp_entities) if config.pptp_entities else []
                veip_list = sorted(config.veip_entities) if config.veip_entities else []

                if ani_list is None or (pptp_list is None and veip_list is None):
                    self.log.warn("no-ani-or-unis")
                    yield self.core_proxy.device_reason_update(self.device_id, 'onu-missing-required-elements')
                    raise Exception("onu-missing-required-elements")

                # Currently logging the ani, pptp, veip, and uni for information purposes.
                # Actually act on the veip/pptp as its ME is the most correct one to use in later tasks.
                # And in some ONU the UNI-G list is incomplete or incorrect...
                for entity_id in ani_list:
                    ani_value = config.ani_g_entities[entity_id]
                    self.log.debug("discovered-ani", entity_id=entity_id, value=ani_value)
                    # TODO: currently only one OLT PON port/ANI, so this works out.  With NGPON there will be 2..?
                    self._total_tcont_count = ani_value.get('total-tcont-count')
                    self.log.debug("set-total-tcont-count", tcont_count=self._total_tcont_count)

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
                    try:
                        yield self._add_uni_port(device, entity_id, uni_id, uni_type)
                        uni_id += 1
                    except AssertionError as e:
                        self.log.warn("could not add UNI", entity_id=entity_id, uni_type=uni_type, e=e)

                self._qos_flexibility = config.qos_configuration_flexibility or 0
                self._omcc_version = config.omcc_version or OMCCVersion.Unknown

                if self._unis:
                    self._dev_info_loaded = True
                else:
                    yield self.core_proxy.device_reason_update(self.device_id, 'no-usable-unis')
                    self.log.warn("no-usable-unis")
                    raise Exception("no-usable-unis")

            except Exception as e:
                self.log.exception('device-info-load', e=e)
                self._deferred = reactor.callLater(_STARTUP_RETRY_WAIT, self._mib_in_sync)

        else:
            self.log.info('device-info-already-loaded', in_sync=in_sync, already_loaded=self._dev_info_loaded)

        if self._dev_info_loaded:
            if device.admin_state == AdminState.PREPROVISIONED or device.admin_state == AdminState.ENABLED:

                @inlineCallbacks
                def success(_results):
                    self.log.info('mib-download-success', _results=_results)
                    yield self.core_proxy.device_state_update(device.id,
                                                              oper_status=OperStatus.ACTIVE,
                                                              connect_status=ConnectStatus.REACHABLE)
                    yield self.core_proxy.device_reason_update(self.device_id, 'initial-mib-downloaded')
                    yield self.enable_ports()
                    self._mib_download_task = None
                    yield self.onu_active_event()

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

                @inlineCallbacks
                def failure(_reason):
                    self.log.warn('mib-download-failure-retrying', _reason=_reason)
                    retry = _STARTUP_RETRY_WAIT * (random.randint(1,5))
                    reactor.callLater(retry, self._mib_in_sync)
                    yield self.core_proxy.device_reason_update(self.device_id, 'initial-mib-download-failure-retrying')

                # start by locking all the unis till mib sync and initial mib is downloaded
                # this way we can capture the port down/up events when we are ready
                self.lock_ports(lock=True)

                # Download an initial mib that creates simple bridge that can pass EAP.  On success (above) finally set
                # the device to active/reachable.   This then opens up the handler to openflow pushes from outside
                self.log.info('downloading-initial-mib-configuration')
                self._mib_download_task = BrcmMibDownloadTask(self.omci_agent, self)
                self._deferred = self._onu_omci_device.task_runner.queue_task(self._mib_download_task)
                self._deferred.addCallbacks(success, failure)
            else:
                self.log.info('admin-down-disabling')
                self.disable(device)
        else:
            self.log.info('device-info-not-loaded-skipping-mib-download')

    @inlineCallbacks
    def _add_uni_port(self, device, entity_id, uni_id, uni_type=UniType.PPTP):
        self.log.debug('add-uni-port')

        uni_no = self.mk_uni_port_num(self._onu_indication.intf_id, self._onu_indication.onu_id, uni_id)

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

        yield self.core_proxy.port_created(device.id, uni_port.get_port())

        self._unis[uni_port.port_number] = uni_port

        self._onu_omci_device.alarm_synchronizer.set_alarm_params(onu_id=self._onu_indication.onu_id,
                                                                  uni_ports=self.uni_ports,
                                                                  serial_number=device.serial_number)

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
            device = yield self.core_proxy.get_device(self.device_id)
            parent_device = yield self.core_proxy.get_device(self.parent_id)
            olt_serial_number = parent_device.serial_number
            raised_ts = arrow.utcnow().timestamp

            self.log.debug("onu-indication-context-data",
                           pon_id=self._onu_indication.intf_id,
                           onu_id=self._onu_indication.onu_id,
                           registration_id=self.device_id,
                           device_id=self.device_id,
                           onu_serial_number=device.serial_number,
                           olt_serial_number=olt_serial_number,
                           raised_ts=raised_ts)

            self.log.debug("Trying-to-raise-onu-active-event")
            OnuActiveEvent(self.events, self.device_id,
                           self._onu_indication.intf_id,
                           device.serial_number,
                           str(self.device_id),
                           olt_serial_number, raised_ts,
                           onu_id=self._onu_indication.onu_id).send(True)
        except Exception as active_event_error:
            self.log.exception('onu-activated-event-error',
                               errmsg=active_event_error.message)

    def lock_ports(self, lock=True):

        def success(response):
            self.log.debug('set-onu-ports-state', lock=lock, response=response)

        def failure(response):
            self.log.error('cannot-set-onu-ports-state', lock=lock, response=response)

        task = BrcmUniLockTask(self.omci_agent, self.device_id, lock=lock)
        self._deferred = self._onu_omci_device.task_runner.queue_task(task)
        self._deferred.addCallbacks(success, failure)
