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
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, TimeoutError, failure
from pyvoltha.adapters.extensions.omci.omci_me import Ont2G, OmciNullPointer, PriorityQueueFrame, \
    Ieee8021pMapperServiceProfileFrame, MacBridgePortConfigurationDataFrame
from pyvoltha.adapters.extensions.omci.tasks.task import Task
from pyvoltha.adapters.extensions.omci.omci_defs import EntityOperations, ReasonCodes
from pyvoltha.adapters.extensions.omci.omci_entities import OntG, Tcont, PriorityQueueG, Ieee8021pMapperServiceProfile, \
    GemPortNetworkCtp

OP = EntityOperations
RC = ReasonCodes

SETUP_TP_TASK_PRIORITY = 240


class TechProfileDownloadFailure(Exception):
    """
    This error is raised by default when the download fails
    """


class TechProfileResourcesFailure(Exception):
    """
    This error is raised by when one or more resources required is not available
    """


class BrcmTpSetupTask(Task):
    """
    OpenOMCI Tech-Profile Download Task

    """

    name = "Broadcom Tech-Profile Download Task"

    def __init__(self, omci_agent, handler, uni_id, tconts, gem_ports, tp_table_id):
        """
        Class initialization

        :param omci_agent: (OmciAdapterAgent) OMCI Adapter agent
        :param handler: (BrcmOpenomciOnuHandler) ONU Device Handler Instance
        :param uni_id: (int) numeric id of the uni port on the onu device, starts at 0
        :param tp_table_id: (int) Technology Profile Table-ID
        """
        super(BrcmTpSetupTask, self).__init__(BrcmTpSetupTask.name,
                                              omci_agent,
                                              handler.device_id,
                                              priority=SETUP_TP_TASK_PRIORITY,
                                              exclusive=True)

        self.log = structlog.get_logger(device_id=handler.device_id,
                                        name=BrcmTpSetupTask.name,
                                        task_id=self._task_id,
                                        tconts=tconts,
                                        gem_ports=gem_ports,
                                        uni_id=uni_id,
                                        tp_table_id=tp_table_id)

        self._onu_device = omci_agent.get_device(handler.device_id)
        self._local_deferred = None

        self._uni_port = handler.uni_ports[uni_id]
        assert self._uni_port.uni_id == uni_id

        # Tech Profile Table ID
        self._tp_table_id = tp_table_id

        # Entity IDs. IDs with values can probably be most anything for most ONUs,
        #             IDs set to None are discovered/set

        self._mac_bridge_service_profile_entity_id = \
            handler.mac_bridge_service_profile_entity_id
        self._ieee_mapper_service_profile_entity_id = \
            handler.pon_port.ieee_mapper_service_profile_entity_id
        self._mac_bridge_port_ani_entity_id = \
            handler.pon_port.mac_bridge_port_ani_entity_id
        self._gal_enet_profile_entity_id = \
            handler.gal_enet_profile_entity_id

        self._tconts = []
        for t in tconts:
            self._tconts.append(t)

        self._gem_ports = []
        for g in gem_ports:
            self._gem_ports.append(g)

        self.tcont_me_to_queue_map = dict()
        self.uni_port_to_queue_map = dict()

    def cancel_deferred(self):
        super(BrcmTpSetupTask, self).cancel_deferred()

        d, self._local_deferred = self._local_deferred, None
        try:
            if d is not None and not d.called:
                d.cancel()
        except:
            pass

    def start(self):
        """
        Start the Tech-Profile Download
        """
        super(BrcmTpSetupTask, self).start()
        self._local_deferred = reactor.callLater(0, self.perform_service_specific_steps)

    def stop(self):
        """
        Shutdown Tech-Profile download tasks
        """
        self.cancel_deferred()
        super(BrcmTpSetupTask, self).stop()

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

        self.log.debug("OMCI Result", operation=operation, omci_msg=omci_msg, status=status, error_mask=error_mask,
                       failed_mask=failed_mask, unsupported_mask=unsupported_mask)

        if status == RC.Success:
            self.strobe_watchdog()
            return True

        elif status == RC.InstanceExists:
            return False

        raise TechProfileDownloadFailure(
            '{} failed with a status of {}, error_mask: {}, failed_mask: {}, '
            'unsupported_mask: {}'.format(operation, status, error_mask, failed_mask, unsupported_mask))

    @inlineCallbacks
    def perform_service_specific_steps(self):
        self.log.info('starting-tech-profile-setup', uni_id=self._uni_port.uni_id,
                      tconts=self._tconts, gem_ports=self._gem_ports, tp_table_id=self._tp_table_id)

        omci_cc = self._onu_device.omci_cc
        gem_pq_associativity = dict()
        pq_to_related_port = dict()
        is_related_ports_configurable = False
        tcont_entity_id = 0

        try:
            ################################################################################
            # TCONTS
            #
            #  EntityID will be referenced by:
            #            - GemPortNetworkCtp
            #  References:
            #            - ONU created TCONT (created on ONU startup)

            # Setup 8021p mapper and ani mac bridge port, if it does not exist
            # Querying just 8021p mapper ME should be enough since we create and
            # delete 8021pMapper and ANI Mac Bridge Port together.
            ieee_8021p_mapper_exists = False
            ieee_8021p_mapper = self._onu_device.query_mib(Ieee8021pMapperServiceProfile.class_id)
            for k, v in ieee_8021p_mapper.items():
                if not isinstance(v, dict):
                    continue
                if k == (self._ieee_mapper_service_profile_entity_id +
                         self._uni_port.mac_bridge_port_num + self._tp_table_id):
                    ieee_8021p_mapper_exists = True

            if ieee_8021p_mapper_exists is False:
                self.log.info("setting-up-8021pmapper-ani-mac-bridge-port")
                yield self._setup__8021p_mapper__ani_mac_bridge_port()

            tcont_idents = self._onu_device.query_mib(Tcont.class_id)
            self.log.debug('tcont-idents', tcont_idents=tcont_idents)

            # There can be only one tcont that can be installed per tech-profile download task
            # Each tech-profile represents a single tcont and associated gemports
            assert len(self._tconts) == 1
            for tcont in self._tconts:
                self.log.debug('tcont-loop', tcont=tcont)

                if tcont.entity_id is None:
                    free_entity_id = None
                    for k, v in tcont_idents.items():
                        if not isinstance(v, dict):
                            continue
                        alloc_check = v.get('attributes', {}).get('alloc_id', 0)
                        # Some onu report both to indicate an available tcont
                        if alloc_check == 0xFF or alloc_check == 0xFFFF:
                            free_entity_id = k
                            break

                    self.log.debug('tcont-loop-free', free_entity_id=free_entity_id, alloc_id=tcont.alloc_id)

                    if free_entity_id is None:
                        self.log.error('no-available-tconts')
                        break

                    # Also assign entity id within tcont object
                    results = yield tcont.add_to_hardware(omci_cc, free_entity_id)
                    self.check_status_and_state(results, 'new-tcont-added')
                    # There is only tcont to be added per tech-profile download procedure
                    # So, there is no issue of overwriting the 'tcont_entity_id'
                    tcont_entity_id = free_entity_id

                else:
                    self.log.debug('tcont-already-assigned', tcont_entity_id=tcont.entity_id, alloc_id=tcont.alloc_id)
                    tcont_entity_id = tcont.entity_id

            ################################################################################
            # GEMS  (GemPortNetworkCtp and GemInterworkingTp)
            #
            #  For both of these MEs, the entity_id is the GEM Port ID. The entity id of the
            #  GemInterworkingTp ME could be different since it has an attribute to specify
            #  the GemPortNetworkCtp entity id.
            #
            #        for the GemPortNetworkCtp ME
            #
            #  GemPortNetworkCtp
            #    EntityID will be referenced by:
            #              - GemInterworkingTp
            #    References:
            #              - TCONT
            #              - Hardcoded upstream TM Entity ID
            #              - (Possibly in Future) Upstream Traffic descriptor profile pointer
            #
            #  GemInterworkingTp
            #    EntityID will be referenced by:
            #              - Ieee8021pMapperServiceProfile
            #    References:
            #              - GemPortNetworkCtp
            #              - Ieee8021pMapperServiceProfile
            #              - GalEthernetProfile
            #

            onu_g = self._onu_device.query_mib(OntG.class_id)
            # If the traffic management option attribute in the ONU-G ME is 0
            # (priority controlled) or 2 (priority and rate controlled), this
            # pointer specifies the priority queue ME serving this GEM port
            # network CTP. If the traffic management option attribute is 1
            # (rate controlled), this attribute redundantly points to the
            # T-CONT serving this GEM port network CTP.
            traffic_mgmt_opt = \
                onu_g.get('attributes', {}).get('traffic_management_options', 0)
            self.log.debug("traffic-mgmt-option", traffic_mgmt_opt=traffic_mgmt_opt)

            prior_q = self._onu_device.query_mib(PriorityQueueG.class_id)
            for k, v in prior_q.items():
                self.log.debug("prior-q", k=k, v=v)

                try:
                    _ = iter(v)
                except TypeError:
                    continue

                # Parse PQ MEs only with relevant information
                if 'instance_id' in v and 'related_port' in v['attributes']:
                    related_port = v['attributes']['related_port']
                    pq_to_related_port[k] = related_port
                    # If the MSB is set, it represents an Upstream PQ referencing the TCONT
                    if v['instance_id'] & 0b1000000000000000:
                        # If it references the TCONT ME we have just installed
                        if tcont_entity_id == (related_port & 0xffff0000) >> 16:
                            if tcont_entity_id not in self.tcont_me_to_queue_map:
                                self.log.debug("prior-q-related-port-and-tcont-me",
                                               related_port=related_port,
                                               tcont_me=tcont_entity_id)
                                self.tcont_me_to_queue_map[tcont_entity_id] = list()
                            # Store the PQ into the list which is referenced by TCONT ME we have provisioned
                            self.tcont_me_to_queue_map[tcont_entity_id].append(k)

                    else:
                        # This represents the PQ pointing to the UNI Port ME (Downstream PQ)
                        if self._uni_port.entity_id == (related_port & 0xffff0000) >> 16:
                            if self._uni_port.entity_id not in self.uni_port_to_queue_map:
                                self.log.debug("prior-q-related-port-and-uni-port-me",
                                               related_port=related_port,
                                               uni_port_me=self._uni_port.entity_id)
                                self.uni_port_to_queue_map[self._uni_port.entity_id] = list()
                            # Store the PQ into the list which is referenced by UNI Port ME we have provisioned
                            self.uni_port_to_queue_map[self._uni_port.entity_id].append(k)

            self.log.debug("ul-prior-q", ul_prior_q=self.tcont_me_to_queue_map)
            self.log.debug("dl-prior-q", dl_prior_q=self.uni_port_to_queue_map)

            for gem_port in self._gem_ports:
                # TODO: Traffic descriptor will be available after meter bands are available
                tcont = gem_port.tcont
                if tcont is None:
                    self.log.error('unknown-tcont-reference', gem_id=gem_port.gem_id)
                    continue

                if gem_port.direction == "upstream" or \
                        gem_port.direction == "bi-directional":

                    # Sort the priority queue list in order of priority.
                    # 0 is highest priority and 0x0fff is lowest.
                    self.tcont_me_to_queue_map[tcont.entity_id].sort()
                    self.uni_port_to_queue_map[self._uni_port.entity_id].sort()
                    # Get the priority queue by indexing the priority value of the gemport.
                    # The self.tcont_me_to_queue_map and self.uni_port_to_queue_map
                    # have priority queue entities ordered in descending order
                    # of priority

                    ul_prior_q_entity_id = \
                        self.tcont_me_to_queue_map[tcont.entity_id][gem_port.priority_q]
                    dl_prior_q_entity_id = \
                        self.uni_port_to_queue_map[self._uni_port.entity_id][gem_port.priority_q]

                    pq_attributes = dict()
                    pq_attributes["pq_entity_id"] = ul_prior_q_entity_id
                    pq_attributes["weight"] = gem_port.weight
                    pq_attributes["scheduling_policy"] = gem_port.scheduling_policy
                    pq_attributes["priority_q"] = gem_port.priority_q
                    gem_pq_associativity[gem_port.entity_id] = pq_attributes

                    assert ul_prior_q_entity_id is not None and \
                           dl_prior_q_entity_id is not None

                    existing = self._onu_device.query_mib(GemPortNetworkCtp.class_id, gem_port.entity_id)
                    self.log.debug('looking-for-gemport-before-create', existing=existing,
                                   class_id=GemPortNetworkCtp.class_id,
                                   entity_id=gem_port.entity_id)
                    if existing:
                        results = yield gem_port.remove_from_hardware(omci_cc)
                        self.check_status_and_state(results, 'remove-existing-gem-port')

                    results = yield gem_port.add_to_hardware(omci_cc,
                                                             tcont.entity_id,
                                                             self._ieee_mapper_service_profile_entity_id +
                                                             self._uni_port.mac_bridge_port_num +
                                                             self._tp_table_id,
                                                             self._gal_enet_profile_entity_id,
                                                             ul_prior_q_entity_id, dl_prior_q_entity_id)
                    self.check_status_and_state(results, 'assign-gem-port')
                elif gem_port.direction == "downstream":
                    # Downstream is inverse of upstream
                    # TODO: could also be a case of multicast. Not supported for now
                    self.log.debug("skipping-downstream-gem", gem_port=gem_port)
                    pass

            ################################################################################
            # Update the PriorityQeue Attributes for the PQ Associated with Gemport
            #
            # Entityt ID was created prior to this call. This is a set
            #
            #

            ont2g = self._onu_device.query_mib(Ont2G.class_id)
            qos_config_flexibility_ie = ont2g.get(0, {}).get('attributes', {}). \
                get('qos_configuration_flexibility', None)
            self.log.debug("qos_config_flexibility",
                           qos_config_flexibility=qos_config_flexibility_ie)

            if qos_config_flexibility_ie & 0b100000:
                is_related_ports_configurable = True

            for k, v in gem_pq_associativity.items():
                if v["scheduling_policy"] == "WRR":
                    self.log.debug("updating-pq-weight")
                    msg = PriorityQueueFrame(v["pq_entity_id"], weight=v["weight"])
                    frame = msg.set()
                    results = yield omci_cc.send(frame)
                    self.check_status_and_state(results, 'set-priority-queues-weight')
                elif v["scheduling_policy"] == "StrictPriority" and \
                        is_related_ports_configurable:
                    self.log.debug("updating-pq-priority")
                    related_port = pq_to_related_port[v["pq_entity_id"]]
                    related_port = related_port & 0xffff0000
                    related_port = related_port | v['priority_q']  # Set priority
                    msg = PriorityQueueFrame(v["pq_entity_id"], related_port=related_port)
                    frame = msg.set()
                    results = yield omci_cc.send(frame)
                    self.check_status_and_state(results, 'set-priority-queues-priority')

            ################################################################################
            # Update the IEEE 802.1p Mapper Service Profile config
            #
            #  EntityID was created prior to this call. This is a set
            #
            #  References:
            #            - Gem Interwork TPs are set here
            #

            gem_entity_ids = [OmciNullPointer] * 8
            # If IEEE8021pMapper ME existed already, then we need to re-build the
            # inter-working-tp-pointers for different gem-entity-ids.
            if ieee_8021p_mapper_exists:
                self.log.debug("rebuilding-interworking-tp-pointers")
                for k, v in ieee_8021p_mapper.items():
                    if not isinstance(v, dict):
                        continue
                    # Check the entity-id of the instance matches what we expect
                    # for this Uni/TechProfileId
                    if k == (self._ieee_mapper_service_profile_entity_id +
                             self._uni_port.mac_bridge_port_num + self._tp_table_id):
                        for i in range(len(gem_entity_ids)):
                            gem_entity_ids[i] = v.get('attributes', {}). \
                                get('interwork_tp_pointer_for_p_bit_priority_' + str(i), OmciNullPointer)
                        self.log.debug("interworking-tp-pointers-rebuilt-after-query-from-onu",
                                       i_w_tp_ptr=gem_entity_ids)

            for gem_port in self._gem_ports:
                self.log.debug("tp-gem-port", entity_id=gem_port.entity_id, uni_id=gem_port.uni_id)

                if gem_port.direction == "upstream" or \
                        gem_port.direction == "bi-directional":
                    for i, p in enumerate(reversed(gem_port.pbit_map)):
                        if p == '1':
                            gem_entity_ids[i] = gem_port.entity_id
                elif gem_port.direction == "downstream":
                    # Downstream gem port p-bit mapper is inverse of upstream
                    # TODO: Could also be a case of multicast. Not supported for now
                    pass

            msg = Ieee8021pMapperServiceProfileFrame(
                (self._ieee_mapper_service_profile_entity_id +
                 self._uni_port.mac_bridge_port_num + self._tp_table_id),
                # 802.1p mapper Service Mapper Profile ID
                interwork_tp_pointers=gem_entity_ids  # Interworking TP IDs
            )
            frame = msg.set()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'set-8021p-mapper-service-profile-ul')

            self.deferred.callback("tech-profile-download-success")

        except TimeoutError as e:
            self.log.warn('rx-timeout-tech-profile', e=e)
            self.deferred.errback(failure.Failure(e))

        except Exception as e:
            self.log.exception('omci-setup-tech-profile', e=e)
            self.deferred.errback(failure.Failure(e))

    @inlineCallbacks
    def _setup__8021p_mapper__ani_mac_bridge_port(self):

        omci_cc = self._onu_device.omci_cc

        try:
            ################################################################################
            # PON Specific                                                                 #
            ################################################################################
            # IEEE 802.1 Mapper Service config - One per tech-profile per UNI
            #
            #  EntityID will be referenced by:
            #            - MAC Bridge Port Configuration Data for the PON port and TP ID
            #  References:
            #            - Nothing at this point. When a GEM port is created, this entity will
            #              be updated to reference the GEM Interworking TP

            msg = Ieee8021pMapperServiceProfileFrame(
                self._ieee_mapper_service_profile_entity_id +
                self._uni_port.mac_bridge_port_num +
                self._tp_table_id
            )
            frame = msg.create()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'create-8021p-mapper-service-profile')

            ################################################################################
            # Create MAC Bridge Port Configuration Data for the PON port via IEEE 802.1
            # mapper service and this per TechProfile. Upon receipt by the ONU, the ONU will create an instance
            # of the following before returning the response.
            #
            #     - MAC bridge port designation data
            #     - MAC bridge port filter table data
            #     - MAC bridge port bridge table data
            #
            #  EntityID will be referenced by:
            #            - Implicitly by the VLAN tagging filter data
            #  References:
            #            - MAC Bridge Service Profile (the bridge)
            #            - IEEE 802.1p mapper service profile for PON port

            # TODO: magic. make a static variable for tp_type
            msg = MacBridgePortConfigurationDataFrame(
                self._mac_bridge_port_ani_entity_id + self._uni_port.entity_id + self._tp_table_id,  # Entity ID
                bridge_id_pointer=(
                        self._mac_bridge_service_profile_entity_id +
                        self._uni_port.mac_bridge_port_num),
                # Bridge Entity ID
                port_num=0xff,  # Port ID - unique number within the bridge
                tp_type=3,  # TP Type (IEEE 802.1p mapper service)
                tp_pointer=(
                        self._ieee_mapper_service_profile_entity_id +
                        self._uni_port.mac_bridge_port_num +
                        self._tp_table_id
                )
                # TP ID, 8021p mapper ID
            )
            frame = msg.create()
            self.log.debug('openomci-msg', omci_msg=msg)
            results = yield omci_cc.send(frame)
            self.check_status_and_state(results, 'create-mac-bridge-port-configuration-data-8021p-mapper')

        except TimeoutError as e:
            self.log.warn('rx-timeout-8021p-ani-port-setup', e=e)
            raise

        except Exception as e:
            self.log.exception('omci-setup-8021p-ani-port-setup', e=e)
            raise
