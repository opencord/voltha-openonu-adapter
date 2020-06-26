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
from voltha_protos.device_pb2 import DeviceType, DeviceTypes
from voltha_protos.voltha_pb2 import ImageDownload, ImageDownloads
from voltha_protos.health_pb2 import HealthStatus

from pyvoltha.adapters.common.frameio.frameio import hexify
from pyvoltha.adapters.extensions.omci.openomci_agent import OpenOMCIAgent, OpenOmciAgentDefaults
from pyvoltha.adapters.extensions.omci.database.mib_db_dict import MibDbVolatileDict
from pyvoltha.adapters.extensions.omci.database.mib_db_dict_lazy import MibDbLazyWriteDict

from brcm_openomci_onu_handler import BrcmOpenomciOnuHandler
from omci.brcm_capabilities_task import BrcmCapabilitiesTask
from copy import deepcopy


@implementer(IAdapterInterface)
class BrcmOpenomciOnuAdapter(object):

    name = 'brcm_openomci_onu'

    supported_device_types = [
        DeviceType(
            id=name,
            vendor_ids=['OPEN', 'ALCL', 'BRCM', 'TWSH', 'ALPH', 'ISKT', 'SFAA', 'BBSM', 'SCOM', 'ARPX', 'DACM', 'ERSN', 'HWTC', 'CIGG'],
            adapter=name,
            accepts_bulk_flow_update=False,
            accepts_add_remove_flow_updates=True
        )
    ]

    def __init__(self, id, core_proxy, adapter_proxy, config, build_info, current_replica, total_replicas, endpoint):
        self.log = structlog.get_logger()
        self.log.debug('starting-adapter', config=config)

        self.core_proxy = core_proxy
        self.adapter_proxy = adapter_proxy
        self.config = config
        self.descriptor = Adapter(
            id=id,
            vendor='VOLTHA OpenONU',
            version=build_info.version,
            config=AdapterConfig(),
            currentReplica=current_replica,
            totalReplicas=total_replicas,
            endpoint=endpoint,
            type=self.name
        )
        self.devices_handlers = dict()
        self.device_handler_class = BrcmOpenomciOnuHandler

        # Customize OpenOMCI for Broadcom ONUs
        self.broadcom_omci = deepcopy(OpenOmciAgentDefaults)

        self.broadcom_omci['mib-synchronizer']['audit-delay'] = 0  # disable audits as brcm onu wont upload once provisioned
        self.broadcom_omci['mib-synchronizer']['database'] = MibDbLazyWriteDict
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
        if not device.id in self.devices_handlers:
            self.devices_handlers[device.id] = BrcmOpenomciOnuHandler(self, device.id)
            reactor.callLater(0, self.devices_handlers[device.id].reconcile, device)
        else:
            self.log.debug('already-called-reconcile-device', device_id=device.id)
        return device

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

    @inlineCallbacks
    def __download_image_success(self, result):
        self.log.debug("__download_image_success")

        onu_device = yield self.core_proxy.get_device(result.id)
        onu_device.reason = 'image-download-success'
        yield self.core_proxy.device_reason_update(result.id, onu_device.reason)

        onu_dev = self._omci_agent.get_device(result.id)
        status =  onu_dev.get_image_download_status(result.name) if onu_dev else None
        image_download = [
            ImageDownload(
                id=result.id,
                name=result.name,
                url=result.url,
                download_state=status.download_state,
                reason=result.reason,
                image_state=result.image_state,
                file_size=status.file_size,
                image_version=result.image_version,
                local_dir=result.local_dir
            )
        ]
        image_downloads = ImageDownloads(items=image_download)

        onu_device.image_downloads.extend(image_download)
        yield self.core_proxy.device_update(onu_device)
        self.log.debug('onu-device-updated', onu_device=onu_device)

    @inlineCallbacks
    def __download_image_fail(self, result):
        self.log.debug("__download_image_fail", failure=result.value)
        onu_device = yield self.core_proxy.get_device(result.value.id)
        onu_device.reason = 'image-download-failed'
        yield self.core_proxy.device_reason_update(result.value.id, onu_device.reason)

        onu_dev = self._omci_agent.get_device(result.value.id)
        status =  onu_dev.get_image_download_status(result.value.name) if onu_dev else None
        image_download = [
            ImageDownload(
                id=result.value.id,
                name=result.value.name,
                url=result.value.url,
                download_state=status.download_state,
                reason=result.value.reason,
                image_state=result.value.image_state,
                file_size=status.file_size,
                image_version=result.value.image_version,
                local_dir=result.value.local_dir
            )
        ]
        image_downloads = ImageDownloads(items=image_download)

        onu_device.image_downloads.extend(image_download)
        yield self.core_proxy.device_update(onu_device)
        self.log.debug('onu-device-updated', onu_device=onu_device)

    def download_image(self, device, request):
        self.log.debug('download-image', device=device, request=request)

        onu_dev = self._omci_agent.get_device(device.id)
        d = onu_dev.do_onu_software_download(request)
        d.addCallbacks(self.__download_image_success, self.__download_image_fail)

    def get_image_download_status(self, device, request):
        self.log.debug('get_image_download_status', device=device, request=request)
        onu_dev = self._omci_agent.get_device(device.id)
        return onu_dev.get_image_download_status(request.name) if onu_dev else None

    def cancel_image_download(self, device, request):
        self.log.debug('cancel_image_download', device=device, request=request)
        onu_dev = self._omci_agent.get_device(device.id)
        onu_dev.cancel_onu_software_download(request.name)

    def activate_image_update(self, device, request):
        self.log.debug('activate_image_update', device=device, request=request)
        onu_dev = self._omci_agent.get_device(device.id)
        d = onu_dev.do_onu_image_activate(request.name)

    def revert_image_update(self, device, request):
        self.log.debug('revert_image_update', device=device, request=request)

    def enable_port(self, device_id, port):
        raise NotImplementedError()

    def disable_port(self, device_id, port):
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
    def update_pm_config(self, device, pm_configs):
        self.log.info("adapter-update-pm-config", device_id=device.id, serial_number=device.serial_number,
                 pm_configs=pm_configs)
        handler = self.devices_handlers[device.id]
        handler.update_pm_config(device, pm_configs)

    def update_flows_bulk(self, device, flows, groups):
        '''
        log.info('bulk-flow-update', device_id=device.id,
                  flows=flows, groups=groups)
        '''
        assert len(groups.items) == 0
        handler = self.devices_handlers[device.id]
        return handler.update_flow_table(device, flows.items)

    def update_flows_incrementally(self, device, flow_changes, group_changes):
        self.log.info('incremental-flow-update', device_id=device.id,
                 flows=flow_changes, groups=group_changes)
        # For now, there is no support for group changes
        assert len(group_changes.to_add.items) == 0
        assert len(group_changes.to_remove.items) == 0

        handler = self.devices_handlers[device.id]
        # Remove flows
        if len(flow_changes.to_remove.items) != 0:
            handler.remove_onu_flows(device, flow_changes.to_remove.items)

        # Add flows
        if len(flow_changes.to_add.items) != 0:
            handler.add_onu_flows(device, flow_changes.to_add.items)

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


    def process_inter_adapter_message(self, msg, max_retry=0, current_retry=0):
        # Unpack the header to know which device needs to handle this message
        if msg.header:
            self.log.debug('process-inter-adapter-message', type=msg.header.type, from_topic=msg.header.from_topic,
                      to_topic=msg.header.to_topic, to_device_id=msg.header.to_device_id)

            # NOTE this should only happen in the case of ONU_IND_REQUEST as described in VOL-3223
            if not msg.header.to_device_id in self.devices_handlers:
                self.log.warn('process-inter-adapter-message-handler-not-found-retry-in-one-sec', type=msg.header.type,
                               from_topic=msg.header.from_topic, to_topic=msg.header.to_topic, to_device_id=msg.header.to_device_id)
                if current_retry == max_retry:
                    self.log.error('process-inter-adapter-message-handler-not-found-no-more-retry', type=msg.header.type,
                               from_topic=msg.header.from_topic, to_topic=msg.header.to_topic, to_device_id=msg.header.to_device_id)
                    return
                else:
                    reactor.callLater(1, self.process_inter_adapter_message, msg, max_retry=10, current_retry=current_retry+1)
                    return

            handler = self.devices_handlers[msg.header.to_device_id]
            handler.process_inter_adapter_message(msg)

    def start_omci_test(self, device, uuid):
        """

        :param device:
        :return:
        """
        self.log.info('Omci-test-action-request-On', request=device)
        handler = self.devices_handlers[device.id]
        result = handler.start_omci_test_action(device, uuid)
        return result
