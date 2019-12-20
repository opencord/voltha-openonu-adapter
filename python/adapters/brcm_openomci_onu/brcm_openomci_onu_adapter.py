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
Broadcom OpenOMCI OLT/ONU adapter.

This adapter does NOT support XPON
"""

from __future__ import absolute_import
import structlog
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from zope.interface import implementer

from pyvoltha.adapters.interface import IAdapterInterface
from voltha_protos.adapter_pb2 import Adapter
from voltha_protos.adapter_pb2 import AdapterConfig
from voltha_protos.common_pb2 import LogLevel
from voltha_protos.device_pb2 import DeviceType, DeviceTypes
from voltha_protos.health_pb2 import HealthStatus

from pyvoltha.adapters.common.frameio.frameio import hexify
from pyvoltha.adapters.extensions.omci.openomci_agent import OpenOMCIAgent, OpenOmciAgentDefaults
from pyvoltha.adapters.extensions.omci.database.mib_db_dict import MibDbVolatileDict

from brcm_openomci_onu_handler import BrcmOpenomciOnuHandler
from omci.brcm_capabilities_task import BrcmCapabilitiesTask
from copy import deepcopy


@implementer(IAdapterInterface)
class BrcmOpenomciOnuAdapter(object):

    name = 'brcm_openomci_onu'

    supported_device_types = [
        DeviceType(
            id=name,
            vendor_ids=['OPEN', 'ALCL', 'BRCM', 'TWSH', 'ALPH', 'ISKT', 'SFAA', 'BBSM', 'SCOM'],
            adapter=name,
            accepts_bulk_flow_update=True
        )
    ]

    def __init__(self, core_proxy, adapter_proxy, config, build_info):
        self.log = structlog.get_logger()
        self.log.debug('starting-adapter', config=config)

        self.core_proxy = core_proxy
        self.adapter_proxy = adapter_proxy
        self.config = config
        self.descriptor = Adapter(
            id=self.name,
            vendor='VOLTHA OpenONU',
            version=build_info.version,
            config=AdapterConfig(log_level=LogLevel.INFO)
        )
        self.devices_handlers = dict()
        self.device_handler_class = BrcmOpenomciOnuHandler

        # Customize OpenOMCI for Broadcom ONUs
        self.broadcom_omci = deepcopy(OpenOmciAgentDefaults)

        self.broadcom_omci['mib-synchronizer']['audit-delay'] = 0  # disable audits as brcm onu wont upload once provisioned
        self.broadcom_omci['mib-synchronizer']['database'] = MibDbVolatileDict
        self.broadcom_omci['alarm-synchronizer']['database'] = MibDbVolatileDict
        self.broadcom_omci['omci-capabilities']['tasks']['get-capabilities'] = BrcmCapabilitiesTask

        # Defer creation of omci agent to a lazy init that allows subclasses to override support classes

    def custom_me_entities(self):
        return None

    @property
    def omci_agent(self):
        if not hasattr(self, '_omci_agent') or self._omci_agent is None:
            self.log.debug('creating-omci-agent')
            self._omci_agent = OpenOMCIAgent(self.core_proxy,
                                             self.adapter_proxy,
                                             support_classes=self.broadcom_omci)
        return self._omci_agent

    def start(self):
        self.log.debug('starting')
        self.omci_agent.start()
        self.log.info('started')

    def stop(self):
        self.log.debug('stopping')

        omci, self._omci_agent = self._omci_agent, None
        if omci is not None:
            self._omci_agent.stop()

        self.log.info('stopped')

    def adapter_descriptor(self):
        return self.descriptor

    def device_types(self):
        return DeviceTypes(items=self.supported_device_types)

    def health(self):
        return HealthStatus(state=HealthStatus.HealthState.HEALTHY)

    def change_master_state(self, master):
        raise NotImplementedError()

    def adopt_device(self, device):
        self.log.info('adopt-device', device_id=device.id)
        self.devices_handlers[device.id] = BrcmOpenomciOnuHandler(self, device.id)
        reactor.callLater(0, self.devices_handlers[device.id].activate, device)
        return device

    def reconcile_device(self, device):
        self.log.info('reconcile-device', device_id=device.id)
        self.devices_handlers[device.id] = BrcmOpenomciOnuHandler(self, device.id)
        reactor.callLater(0, self.devices_handlers[device.id].reconcile, device)

    def abandon_device(self, device):
        raise NotImplementedError()

    def disable_device(self, device):
        self.log.info('disable-onu-device', device_id=device.id)
        if device.id in self.devices_handlers:
            handler = self.devices_handlers[device.id]
            if handler is not None:
                handler.disable(device)

    def reenable_device(self, device):
        self.log.info('reenable-onu-device', device_id=device.id)
        if device.id in self.devices_handlers:
            handler = self.devices_handlers[device.id]
            if handler is not None:
                handler.reenable(device)

    def reboot_device(self, device):
        self.log.info('reboot-device', device_id=device.id)
        if device.id in self.devices_handlers:
            handler = self.devices_handlers[device.id]
            if handler is not None:
                handler.reboot()

    def download_image(self, device, request):
        raise NotImplementedError()

    def get_image_download_status(self, device, request):
        raise NotImplementedError()

    def cancel_image_download(self, device, request):
        raise NotImplementedError()

    def activate_image_update(self, device, request):
        raise NotImplementedError()

    def revert_image_update(self, device, request):
        raise NotImplementedError()

    def self_test_device(self, device):
        """
        This is called to Self a device based on a NBI call.
        :param device: A Voltha.Device object.
        :return: Will return result of self test
        """
        self.log.info('self-test-device - Not implemented yet', device_id=device.id, serial_number=device.serial_number)
        raise NotImplementedError()

    def delete_device(self, device):
        self.log.info('delete-device', device_id=device.id)
        if device.id in self.devices_handlers:
            handler = self.devices_handlers[device.id]
            if handler is not None:
                handler.delete(device)
            del self.devices_handlers[device.id]
        return

    def get_device_details(self, device):
        raise NotImplementedError()

    # TODO(smbaker): When BrcmOpenomciOnuAdapter is updated to inherit from OnuAdapter, this function can be deleted
    def update_pm_config(self, device, pm_config):
        self.log.info("adapter-update-pm-config", device_id=device.id, serial_number=device.serial_number,
                 pm_config=pm_config)
        handler = self.devices_handlers[device.id]
        handler.update_pm_config(device, pm_config)

    def update_flows_bulk(self, device, flows, groups):
        '''
        log.info('bulk-flow-update', device_id=device.id,
                  flows=flows, groups=groups)
        '''
        assert len(groups.items) == 0
        handler = self.devices_handlers[device.id]
        return handler.update_flow_table(device, flows.items)

    def update_flows_incrementally(self, device, flow_changes, group_changes):
        raise NotImplementedError()

    def send_proxied_message(self, proxy_address, msg):
        self.log.debug('send-proxied-message', proxy_address=proxy_address, msg=msg)

    @inlineCallbacks
    def receive_proxied_message(self, proxy_address, msg):
        self.log.debug('receive-proxied-message', proxy_address=proxy_address,
                 device_id=proxy_address.device_id, msg=hexify(msg))
        # Device_id from the proxy_address is the olt device id. We need to
        # get the onu device id using the port number in the proxy_address
        device = self.core_proxy. \
            get_child_device_with_proxy_address(proxy_address)
        if device:
            handler = self.devices_handlers[device.id]
            handler.receive_message(msg)

    def receive_packet_out(self, logical_device_id, egress_port_no, msg):
        self.log.debug('packet-out', logical_device_id=logical_device_id,
                 egress_port_no=egress_port_no, msg_len=len(msg))

    @inlineCallbacks
    def receive_inter_adapter_message(self, msg):
        self.log.debug('receive-inter-adapter-message', msg=msg)
        proxy_address = msg['proxy_address']
        assert proxy_address is not None
        # Device_id from the proxy_address is the olt device id. We need to
        # get the onu device id using the port number in the proxy_address
        device = self.core_proxy. \
            get_child_device_with_proxy_address(proxy_address)
        if device:
            handler = self.devices_handlers[device.id]
            handler.event_messages.put(msg)
        else:
            self.log.error("device-not-found")

    def get_ofp_port_info(self, device, port_no):
        ofp_port_info = self.devices_handlers[device.id].get_ofp_port_info(device, port_no)
        self.log.debug('get-ofp-port-info', device_id=device.id,
                  port_name=ofp_port_info.port.ofp_port.name, port_no=ofp_port_info.port.device_port_no)
        return ofp_port_info

    def process_inter_adapter_message(self, msg):
        # Unpack the header to know which device needs to handle this message
        if msg.header:
            self.log.debug('process-inter-adapter-message', type=msg.header.type, from_topic=msg.header.from_topic,
                      to_topic=msg.header.to_topic, to_device_id=msg.header.to_device_id)
            handler = self.devices_handlers[msg.header.to_device_id]
            handler.process_inter_adapter_message(msg)

    def create_interface(self, device, data):
        self.log.debug('create-interface', device_id=device.id)
        if device.id in self.devices_handlers:
            handler = self.devices_handlers[device.id]
            if handler is not None:
                handler.create_interface(data)

    def update_interface(self, device, data):
        self.log.debug('update-interface', device_id=device.id)
        if device.id in self.devices_handlers:
            handler = self.devices_handlers[device.id]
            if handler is not None:
                handler.update_interface(data)

    def remove_interface(self, device, data):
        self.log.debug('remove-interface', device_id=device.id)
        if device.id in self.devices_handlers:
            handler = self.devices_handlers[device.id]
            if handler is not None:
                handler.remove_interface(data)
