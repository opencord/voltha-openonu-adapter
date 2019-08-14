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
from pyvoltha.adapters.extensions.omci.omci_me import PptpEthernetUniFrame, GalEthernetProfileFrame, \
    MacBridgePortConfigurationDataFrame, MacBridgeServiceProfileFrame, Ieee8021pMapperServiceProfileFrame, \
    VeipUniFrame, ExtendedVlanTaggingOperationConfigurationDataFrame
from pyvoltha.adapters.extensions.omci.tasks.task import Task
from pyvoltha.adapters.extensions.omci.omci_defs import EntityOperations, ReasonCodes
from uni_port import UniType
from pon_port \
    import TASK_PRIORITY, DEFAULT_TPID, DEFAULT_GEM_PAYLOAD

OP = EntityOperations
RC = ReasonCodes


class MibDownloadFailure(Exception):
    """
    This error is raised by default when the download fails
    """


class MibResourcesFailure(Exception):
    """
    This error is raised by when one or more resources required is not available
    """


class BrcmMibDownloadTask(Task):
    """
    OpenOMCI MIB Download Bridge Setup Task

    This task takes the legacy OMCI 'script' for provisioning the Broadcom ONU
    and converts it to run as a Task on the OpenOMCI Task runner.  This is
    in order to begin to decompose service instantiation in preparation for
    Technology Profile work.

    Once technology profiles are ready, some of this task may hang around or
    be moved into OpenOMCI if there are any very common settings/configs to do
    for any profile that may be provided in the v2.0 release

    """

    name = "Broadcom MIB Download Bridge Setup Task"

    def __init__(self, omci_agent, handler):
        """
        Class initialization

        :param omci_agent: (OmciAdapterAgent) OMCI Adapter agent
        :param device_id: (str) ONU Device ID
        """

        self.log = structlog.get_logger(device_id=handler.device_id)
        self.log.debug('function-entry')

        super(BrcmMibDownloadTask, self).__init__(BrcmMibDownloadTask.name,
                                                  omci_agent,
                                                  handler.device_id,
                                                  priority=TASK_PRIORITY)
        self._handler = handler
        self._onu_device = omci_agent.get_device(handler.device_id)
        self._local_deferred = None

        # Frame size
        self._max_gem_payload = DEFAULT_GEM_PAYLOAD

        self._pon = handler.pon_port

        # Defaults
        self._input_tpid = DEFAULT_TPID
        self._output_tpid = DEFAULT_TPID

        # Entity IDs. IDs with values can probably be most anything for most ONUs,
        #             IDs set to None are discovered/set

        self._mac_bridge_service_profile_entity_id = \
            self._handler.mac_bridge_service_profile_entity_id
        self._ieee_mapper_service_profile_entity_id = \
            self._pon.ieee_mapper_service_profile_entity_id
        self._mac_bridge_port_ani_entity_id = \
            self._pon.mac_bridge_port_ani_entity_id
        self._gal_enet_profile_entity_id = \
            self._handler.gal_enet_profile_entity_id

        self._free_ul_prior_q_entity_ids = set()
        self._free_dl_prior_q_entity_ids = set()

    def cancel_deferred(self):
        self.log.debug('function-entry')
        super(BrcmMibDownloadTask, self).cancel_deferred()

        d, self._local_deferred = self._local_deferred, None
        try:
            if d is not None and not d.called:
                d.cancel()
        except:
            pass

    def start(self):
        """
        Start the MIB Download
        """
        self.log.debug('function-entry')
        super(BrcmMibDownloadTask, self).start()
        self._local_deferred = reactor.callLater(0, self.perform_mib_download)

    def stop(self):
        """
        Shutdown MIB Synchronization tasks
        """
        self.log.debug('function-entry')
        self.log.debug('stopping')

        self.cancel_deferred()
        super(BrcmMibDownloadTask, self).stop()

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

        self.log.debug("OMCI Result", operation=operation,
                       omci_msg=omci_msg, status=status,
                       error_mask=error_mask, failed_mask=failed_mask,
                       unsupported_mask=unsupported_mask)

        if status == RC.Success:
            self.strobe_watchdog()
            return True

        elif status == RC.InstanceExists:
            return False

        raise MibDownloadFailure('{} failed with a status of {}, error_mask: {}, failed_mask: {}, unsupported_mask: {}'
                                 .format(operation, status, error_mask, failed_mask, unsupported_mask))

    @inlineCallbacks
    def perform_mib_download(self):
        """
        Send the commands to minimally configure the PON, Bridge, and
        UNI ports for this device. The application of any service flows
        and other characteristics are done as needed.
        """
        try:
            self.log.debug('function-entry')
            self.log.info('perform-download')

            if self._handler.enabled and len(self._handler.uni_ports) > 0:
                yield self._handler.core_proxy.device_reason_update(self.device_id, 'performing-initial-mib-download')

            try:
                # Lock the UNI ports to prevent any alarms during initial configuration
                # of the ONU
                self.strobe_watchdog()

                # Provision the initial bridge configuration
                yield self.perform_initial_bridge_setup()

                for uni_port in self._handler.uni_ports:
                    # Provision the initial bridge configuration
                    yield self.perform_uni_initial_bridge_setup(uni_port)

                self.deferred.callback('initial-download-success')

            except TimeoutError as e:
                self.log.error('initial-download-failure', e=e)
                self.deferred.errback(failure.Failure(e))

            except Exception as e:
                self.log.exception('initial-download-failure', e=e)
                self.deferred.errback(failure.Failure(e))

            else:
                e = MibResourcesFailure('Required resources are not available',
                                        len(self._handler.uni_ports))
                self.deferred.errback(failure.Failure(e))
        except BaseException as e:
            self.log.debug('cannot-start-mib-download', exception=e)

    @inlineCallbacks
    def perform_initial_bridge_setup(self):
        self.log.debug('function-entry')

        omci_cc = self._onu_device.omci_cc
        try:
            ########################################################################################
            # Create GalEthernetProfile - Once per ONU/PON interface
            #
            #  EntityID will be referenced by:
            #            - GemInterworkingTp
            #  References:
            #            - Nothing

            msg = GalEthernetProfileFrame(
                self._gal_enet_profile_entity_id,
                max_gem_payload_size=self._max_gem_payload
            )
            frame = msg.create()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'create-gal-ethernet-profile')

        except TimeoutError as e:
            self.log.warn('rx-timeout-initial-gal-profile', e=e)
            raise

        except Exception as e:
            self.log.exception('omci-setup-initial-gal-profile', e=e)
            raise

        returnValue(None)

    @inlineCallbacks
    def perform_uni_initial_bridge_setup(self, uni_port):
        self.log.debug('function-entry')
        omci_cc = self._onu_device.omci_cc
        frame = None
        try:
            ################################################################################
            # Common - PON and/or UNI                                                      #
            ################################################################################
            # MAC Bridge Service Profile
            #
            #  EntityID will be referenced by:
            #            - MAC Bridge Port Configuration Data (PON & UNI)
            #  References:
            #            - Nothing

            # TODO: magic. event if static, assign to a meaningful variable name
            attributes = {
                'spanning_tree_ind': False,
                'learning_ind': True,
                'priority': 0x8000,
                'max_age': 20 * 256,
                'hello_time': 2 * 256,
                'forward_delay': 15 * 256,
                'unknown_mac_address_discard': True
            }
            msg = MacBridgeServiceProfileFrame(
                self._mac_bridge_service_profile_entity_id + uni_port.mac_bridge_port_num,
                attributes
            )
            frame = msg.create()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'create-mac-bridge-service-profile')

            ################################################################################
            # UNI Specific                                                                 #
            ################################################################################
            # MAC Bridge Port config
            # This configuration is for Ethernet UNI
            #
            #  EntityID will be referenced by:
            #            - Nothing
            #  References:
            #            - MAC Bridge Service Profile (the bridge)
            #            - PPTP Ethernet or VEIP UNI

            # default to PPTP
            tp_type = 1
            if uni_port.type.value == UniType.VEIP.value:
                tp_type = 11
            elif uni_port.type.value == UniType.PPTP.value:
                tp_type = 1

            msg = MacBridgePortConfigurationDataFrame(
                self._mac_bridge_port_ani_entity_id + uni_port.entity_id,  # Entity ID
                bridge_id_pointer=self._mac_bridge_service_profile_entity_id + uni_port.mac_bridge_port_num,  # Bridge Entity ID
                port_num=uni_port.mac_bridge_port_num,   # Port ID
                tp_type=tp_type,                         # PPTP Ethernet or VEIP UNI
                tp_pointer=uni_port.entity_id            # Ethernet UNI ID
            )
            frame = msg.create()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'create-mac-bridge-port-configuration-data-uni-port')

        except TimeoutError as e:
            self.log.warn('rx-timeout-inital-per-uni-setup', e=e)
            raise

        except Exception as e:
            self.log.exception('omci-setup-initial-per-uni-setup', e=e)
            raise

        returnValue(None)
