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
from enum import Enum
from pyvoltha.common.utils.nethelpers import mac_str_to_tuple
from voltha_protos.common_pb2 import OperStatus, AdminState
from voltha_protos.device_pb2 import Port
from voltha_protos.openflow_13_pb2 import OFPPF_1GB_FD, OFPPF_FIBER, OFPPS_LINK_DOWN, OFPPS_LIVE, ofp_port


class UniType(Enum):
    """
    UNI Types Defined in G.988
    """
    PPTP = 'PhysicalPathTerminationPointEthernet'
    VEIP = 'VirtualEthernetInterfacePoint'
    # TODO: Add others as they become supported


# ReservedVlan Transparent Vlan (Masked Vlan, VLAN_ANY in ONOS Flows)

RESERVED_TRANSPARENT_VLAN = 4096


class UniPort(object):
    """Wraps southbound-port(s) support for ONU"""

    def __init__(self, handler, name, uni_id, port_no, ofp_port_no,
                 parent_port_no, device_serial_number,
                 type=UniType.PPTP):
        self.log = structlog.get_logger(device_id=handler.device_id,
                                        port_no=port_no)
        self._enabled = False
        self._handler = handler
        self._name = name
        self._port = None
        self._port_number = port_no
        self._ofp_port_no = ofp_port_no
        self._entity_id = None
        self._mac_bridge_port_num = 0
        self._type = type
        self._uni_id = uni_id
        self._parent_port_no = parent_port_no
        self._serial_num = device_serial_number

        self._admin_state = AdminState.DISABLED
        self._oper_status = OperStatus.DISCOVERED

    def __str__(self):
        return "UniPort - name: {}, port_number: {}, admin_state: {}, oper_state: {}, entity_id: {}, " \
               "mac_bridge_port_num: {}, type: {}, ofp_port: {}" \
            .format(self.name, self.port_number, self.adminstate, self.operstatus, self.entity_id,
                    self._mac_bridge_port_num, self.type, self._ofp_port_no)

    def __repr__(self):
        return str(self)

    @staticmethod
    def create(handler, name, uni_id, port_no, ofp_port_no,
               parent_port_no, device_serial_number, type):
        port = UniPort(handler, name, uni_id, port_no, ofp_port_no,
                       parent_port_no, device_serial_number, type)
        return port

    def _start(self):
        self._cancel_deferred()
        self._admin_state = AdminState.ENABLED

    def _stop(self):
        self._cancel_deferred()
        self._admin_state = AdminState.DISABLED
        self._oper_status = OperStatus.UNKNOWN

    def delete(self):
        self.enabled = False
        self._handler = None

    def _cancel_deferred(self):
        pass

    @property
    def adminstate(self):
        return self._admin_state

    @adminstate.setter
    def adminstate(self, value):
        self._admin_state = value

    @property
    def operstatus(self):
        return self._oper_status

    @operstatus.setter
    def operstatus(self, value):
        self._oper_status = value

    @property
    def name(self):
        return self._name

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, value):
        if self._enabled != value:
            self._enabled = value

            if value:
                self._start()
            else:
                self._stop()

    @property
    def uni_id(self):
        """
        Physical prt index on ONU 0 - N
        :return: (int) uni id
        """
        return self._uni_id

    @property
    def mac_bridge_port_num(self):
        """
        Port number used when creating MacBridgePortConfigurationDataFrame port number
        :return: (int) port number
        """
        return self._mac_bridge_port_num

    @mac_bridge_port_num.setter
    def mac_bridge_port_num(self, value):
        self._mac_bridge_port_num = value

    @property
    def port_number(self):
        """
        Physical device port number
        :return: (int) port number
        """
        return self._port_number

    @property
    def entity_id(self):
        """
        OMCI UNI_G entity ID for port
        """
        return self._entity_id

    @entity_id.setter
    def entity_id(self, value):
        assert self._entity_id is None, 'Cannot reset the Entity ID'
        self._entity_id = value

    @property
    def type(self):
        """
        UNI Type used in OMCI messaging
        :return: (UniType) One of the enumerated types
        """
        return self._type

    @property
    def parent_port_no(self):
        """
        Parent port number for this ONU - will be the PON port of the OLT
        where this ONU is connected
        """
        return self._parent_port_no

    @property
    def serial_number(self):
        """
        Serial number of the ONU
        """
        return self._serial_num

    def get_port(self):
        """
        Get the VOLTHA PORT object for this port
        :return: VOLTHA Port object
        """

        cap = OFPPF_1GB_FD | OFPPF_FIBER

        hw_addr = mac_str_to_tuple('08:%02x:%02x:%02x:%02x:%02x' %
                                   ((self.parent_port_no >> 8 & 0xff),
                                    self.parent_port_no & 0xff,
                                    (self.port_number >> 16) & 0xff,
                                    (self.port_number >> 8) & 0xff,
                                    self.port_number & 0xff))

        name = self.serial_number + '-' + str(self.mac_bridge_port_num)

        ofstate = OFPPS_LINK_DOWN
        if self.operstatus is OperStatus.ACTIVE:
            ofstate = OFPPS_LIVE

        self._port = Port(port_no=self.port_number,
                          label=self.port_id_name(),
                          type=Port.ETHERNET_UNI,
                          admin_state=self._admin_state,
                          oper_status=self._oper_status,
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
                          ))
        return self._port

    def port_id_name(self):
        return 'uni-{}'.format(self._port_number)
