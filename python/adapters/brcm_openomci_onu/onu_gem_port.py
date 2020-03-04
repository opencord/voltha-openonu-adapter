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
from twisted.internet.defer import inlineCallbacks, returnValue
from pyvoltha.adapters.extensions.omci.omci_me import GemInterworkingTpFrame, GemPortNetworkCtpFrame, MulticastGemInterworkingTPFrame
from pyvoltha.adapters.extensions.omci.omci_entities import IPv4MulticastAddressTable
from pyvoltha.adapters.extensions.omci.omci_defs import ReasonCodes

RC = ReasonCodes


class OnuGemPort(object):
    """
    Broadcom ONU specific implementation
    """

    def __init__(self, gem_id, uni_id, alloc_id,
                 entity_id=None,
                 direction="BIDIRECTIONAL",
                 encryption=False,
                 discard_config=None,
                 discard_policy=None,
                 max_q_size="auto",
                 pbit_map="0b00000011",
                 priority_q=3,
                 scheduling_policy="WRR",
                 weight=8,
                 omci_transport=False,
                 multicast=False,
                 tcont_ref=None,
                 traffic_class=None,
                 intf_ref=None,
                 untagged=False,
                 name=None,
                 handler=None):

        self.log = structlog.get_logger(device_id=handler.device_id, uni_id=uni_id, gem_id=gem_id)

        self.name = name
        self.gem_id = gem_id
        self.uni_id = uni_id
        self._alloc_id = alloc_id
        self.tcont_ref = tcont_ref
        self.intf_ref = intf_ref
        self.traffic_class = traffic_class
        self._direction = None
        self._encryption = encryption
        self._discard_config = None
        self._discard_policy = None
        self._max_q_size = None
        self._pbit_map = None
        self._scheduling_policy = None
        self._omci_transport = omci_transport
        self.multicast = multicast
        self.untagged = untagged
        self._handler = handler

        self.direction = direction
        self.encryption = encryption
        self.discard_config = discard_config
        self.discard_policy = discard_policy
        self.max_q_size = max_q_size
        self.pbit_map = pbit_map
        self.priority_q = priority_q
        self.scheduling_policy = scheduling_policy
        self.weight = weight

        self._pon_id = None
        self._onu_id = None
        self._entity_id = entity_id

        # Statistics
        self.rx_packets = 0
        self.rx_bytes = 0
        self.tx_packets = 0
        self.tx_bytes = 0

    def __str__(self):
        return "OnuGemPort - entity_id {}, alloc-id: {}, gem-id: {}, direction: {}, multicast: {} ".format(self.entity_id, self.alloc_id,
                                                                              self.gem_id, self.direction, self.multicast)

    def __repr__(self):
        return str(self)

    @property
    def mcast(self):
        return self.multicast

    @property
    def pon_id(self):
        return self._pon_id

    @pon_id.setter
    def pon_id(self, pon_id):
        assert self._pon_id is None or self._pon_id == pon_id, 'PON-ID can only be set once'
        self._pon_id = pon_id

    @property
    def onu_id(self):
        return self._onu_id

    @onu_id.setter
    def onu_id(self, onu_id):
        assert self._onu_id is None or self._onu_id == onu_id, 'ONU-ID can only be set once'
        self._onu_id = onu_id

    @property
    def alloc_id(self):
        return self._alloc_id

    @property
    def direction(self):
        return self._direction

    @direction.setter
    def direction(self, direction):
        # GEM Port CTP are configured separately in UPSTREAM and DOWNSTREAM.
        # BIDIRECTIONAL is not supported.
        assert direction == "UPSTREAM" or direction == "DOWNSTREAM" or \
               direction == "BIDIRECTIONAL", "invalid-direction"

        # OMCI framework expects string in lower-case. Tech-Profile sends in upper-case.
        if direction == "UPSTREAM":
            self._direction = "upstream"
        elif direction == "DOWNSTREAM":
            self._direction = "downstream"
        elif direction == "BIDIRECTIONAL":
            self._direction = "bi-directional"

    @property
    def tcont(self):
        tcont_item = self._handler.pon_port.tconts.get(self.alloc_id)
        return tcont_item

    @property
    def omci_transport(self):
        return self._omci_transport

    def to_dict(self):
        return {
            'port-id': self.gem_id,
            'alloc-id': self.alloc_id,
            'encryption': self._encryption,
            'omci-transport': self.omci_transport
        }

    @property
    def entity_id(self):
        return self._entity_id

    @entity_id.setter
    def entity_id(self, value):
        self._entity_id = value

    @property
    def encryption(self):
        return self._encryption

    @encryption.setter
    def encryption(self, value):
        # FIXME The encryption should come as boolean by default
        value = eval(value)
        assert isinstance(value, bool), 'encryption is a boolean'

        if self._encryption != value:
            self._encryption = value

    @property
    def discard_config(self):
        return self._discard_config

    @discard_config.setter
    def discard_config(self, discard_config):
        assert isinstance(discard_config, dict), "discard_config not dict"
        assert 'max_probability' in discard_config, "max_probability missing"
        assert 'max_threshold' in discard_config, "max_threshold missing"
        assert 'min_threshold' in discard_config, "min_threshold missing"
        self._discard_config = discard_config

    @property
    def discard_policy(self):
        return self._discard_policy

    @discard_policy.setter
    def discard_policy(self, discard_policy):
        dp = ("TailDrop", "WTailDrop", "RED", "WRED")
        assert (isinstance(discard_policy, str))
        assert (discard_policy in dp)
        self._discard_policy = discard_policy

    @property
    def max_q_size(self):
        return self._max_q_size

    @max_q_size.setter
    def max_q_size(self, max_q_size):
        if isinstance(max_q_size, str):
            assert (max_q_size == "auto")
        else:
            assert (isinstance(max_q_size, int))

        self._max_q_size = max_q_size

    @property
    def pbit_map(self):
        return self._pbit_map

    @pbit_map.setter
    def pbit_map(self, pbit_map):
        assert (isinstance(pbit_map, str))
        assert (len(pbit_map[2:]) == 8)  # Example format of pbit_map: "0b00000101"
        try:
            _ = int(pbit_map[2], 2)
        except ValueError:
            raise Exception("pbit_map-not-binary-string-{}".format(pbit_map))

        # remove '0b'
        self._pbit_map = pbit_map[2:]

    @property
    def scheduling_policy(self):
        return self._scheduling_policy

    @scheduling_policy.setter
    def scheduling_policy(self, scheduling_policy):
        sp = ("WRR", "StrictPriority")
        assert (isinstance(scheduling_policy, str))
        assert (scheduling_policy in sp)
        self._scheduling_policy = scheduling_policy

    @staticmethod
    def create(handler, gem_port):

        return OnuGemPort(gem_id=gem_port['gemport_id'],
                          uni_id=gem_port['uni_id'],
                          alloc_id=gem_port['alloc_id_ref'],
                          direction=gem_port['direction'],
                          encryption=gem_port['encryption'],  # aes_indicator,
                          discard_config=gem_port['discard_config'],
                          discard_policy=gem_port['discard_policy'],
                          max_q_size=gem_port['max_q_size'],
                          pbit_map=gem_port['pbit_map'],
                          priority_q=gem_port['priority_q'],
                          scheduling_policy=gem_port['scheduling_policy'],
                          weight=gem_port['weight'],
                          multicast=gem_port['is_multicast'],
                          handler=handler,
                          untagged=False)

    @inlineCallbacks
    def add_to_hardware(self, omci,
                        tcont_entity_id,
                        ieee_mapper_service_profile_entity_id,
                        gal_enet_profile_entity_id,
                        ul_prior_q_entity_id,
                        dl_prior_q_entity_id):

        self.log.debug('add-to-hardware', entity_id=self.entity_id, gem_id=self.gem_id,
                       tcont_entity_id=tcont_entity_id,
                       ieee_mapper_service_profile_entity_id=ieee_mapper_service_profile_entity_id,
                       gal_enet_profile_entity_id=gal_enet_profile_entity_id,
                       ul_prior_q_entity_id=ul_prior_q_entity_id,
                       dl_prior_q_entity_id=dl_prior_q_entity_id,
                       multicast=self.multicast)

        try:
            direction = "downstream" if self.multicast else "bi-directional"
            entity_id = self.gem_id if self.multicast else self.entity_id

            attributes = dict()
            attributes['priority_queue_pointer_downstream'] = dl_prior_q_entity_id
            msg = GemPortNetworkCtpFrame(
                entity_id,  # same entity id as GEM port
                port_id=self.gem_id,
                tcont_id=tcont_entity_id,
                direction=direction,
                upstream_tm=ul_prior_q_entity_id,
                attributes=attributes
            )
            frame = msg.create()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci.send(frame)
            self.check_status_and_state(results, 'create-gem-port-network-ctp')

        except Exception as e:
            self.log.exception('gemport-create', e=e)
            raise

        try:
            if self.multicast:
                # 224.0.0.0 - 239.255.255.255 is valid multicast ip range
                mcast_ip_list= IPv4MulticastAddressTable( gem_port_id=self.gem_id,
                                                          secondary_key=0,
                                                          multicast_ip_range_start="224.0.0.0",
                                                          multicast_ip_range_stop="239.255.255.255")
                msg = MulticastGemInterworkingTPFrame(
                      self.gem_id,
                      gem_port_network_ctp_pointer=self.gem_id,
                      interworking_option=0,  # Mac Bridge
                      service_profile_pointer=0,
                      gal_profile_pointer=gal_enet_profile_entity_id,
                      ipv4_multicast_address_table= mcast_ip_list
                    )
            else:
                msg = GemInterworkingTpFrame(
                    self.entity_id,  # same entity id as GEM port
                    gem_port_network_ctp_pointer=self.entity_id,
                    interworking_option=5,  # IEEE 802.1
                    service_profile_pointer=ieee_mapper_service_profile_entity_id,
                    interworking_tp_pointer=0x0,
                    pptp_counter=1,
                    gal_profile_pointer=gal_enet_profile_entity_id
                )
            frame = msg.create()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci.send(frame)
            self.check_status_and_state(results, 'create-gem-interworking-tp')

        except Exception as e:
            self.log.exception('interworking-create', e=e)
            raise

        returnValue(results)

    @inlineCallbacks
    def remove_from_hardware(self, omci):
        self.log.debug('remove-from-hardware', gem_id=self.gem_id)

        try:
            msg = GemInterworkingTpFrame(self.entity_id)
            frame = msg.delete()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci.send(frame)
            self.check_status_and_state(results, 'delete-gem-port-network-ctp')
        except Exception as e:
            self.log.exception('interworking-delete', e=e)
            raise

        try:
            msg = GemPortNetworkCtpFrame(self.entity_id)
            frame = msg.delete()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci.send(frame)
            self.check_status_and_state(results, 'delete-gem-interworking-tp')
        except Exception as e:
            self.log.exception('gemport-delete', e=e)
            raise

        returnValue(results)

    def check_status_and_state(self, results, operation=''):
        omci_msg = results.fields['omci_message'].fields
        status = omci_msg['success_code']
        error_mask = omci_msg.get('parameter_error_attributes_mask', 'n/a')
        failed_mask = omci_msg.get('failed_attributes_mask', 'n/a')
        unsupported_mask = omci_msg.get('unsupported_attributes_mask', 'n/a')

        self.log.debug("OMCI Result", operation=operation, omci_msg=omci_msg,
                       status=status, error_mask=error_mask,
                       failed_mask=failed_mask, unsupported_mask=unsupported_mask)

        if status == RC.Success:
            return True

        elif status == RC.InstanceExists:
            return False
