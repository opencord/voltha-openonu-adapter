#!/usr/bin/env python
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

"""OpenONU Adapter main entry point"""

from __future__ import absolute_import
import argparse
import os
import time
import types
import sys

import arrow
import yaml
import socketserver
import configparser

from simplejson import dumps
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet.task import LoopingCall
from zope.interface import implementer

from pyvoltha.common.structlog_setup import setup_logging, update_logging
from pyvoltha.common.utils.asleep import asleep
from pyvoltha.common.utils.deferred_utils import TimeOutError
from pyvoltha.common.utils.dockerhelpers import get_my_containers_name
from pyvoltha.common.utils.nethelpers import get_my_primary_local_ipv4, \
    get_my_primary_interface
from pyvoltha.common.utils.registry import registry, IComponent
from pyvoltha.adapters.kafka.adapter_proxy import AdapterProxy
from pyvoltha.adapters.kafka.adapter_request_facade import AdapterRequestFacade
from pyvoltha.adapters.kafka.core_proxy import CoreProxy
from pyvoltha.adapters.kafka.kafka_inter_container_library import IKafkaMessagingProxy, \
    get_messaging_proxy
from pyvoltha.adapters.kafka.kafka_proxy import KafkaProxy, get_kafka_proxy
from voltha_protos.adapter_pb2 import AdapterConfig

from brcm_openomci_onu_adapter import BrcmOpenomciOnuAdapter
from probe import Probe

defs = dict(
    build_info_file='./BUILDINFO',
    config=os.environ.get('CONFIG', './openonu.yml'),
    container_name_regex=os.environ.get('CONTAINER_NUMBER_EXTRACTOR', '^.*\.(['
                                                                      '0-9]+)\..*$'),
    consul=os.environ.get('CONSUL', 'localhost:8500'),
    name=os.environ.get('NAME', 'openonu'),
    vendor=os.environ.get('VENDOR', 'Voltha Project'),
    device_type=os.environ.get('DEVICE_TYPE', 'openonu'),
    accept_bulk_flow=os.environ.get('ACCEPT_BULK_FLOW', True),
    accept_atomic_flow=os.environ.get('ACCEPT_ATOMIC_FLOW', True),
    etcd=os.environ.get('ETCD', 'localhost:2379'),
    core_topic=os.environ.get('CORE_TOPIC', 'rwcore'),
    event_topic=os.environ.get('EVENT_TOPIC', 'voltha.events'),
    interface=os.environ.get('INTERFACE', get_my_primary_interface()),
    instance_id=os.environ.get('INSTANCE_ID', os.environ.get('HOSTNAME', '1')),
    kafka_adapter=os.environ.get('KAFKA_ADAPTER', '192.168.0.20:9092'),
    kafka_cluster=os.environ.get('KAFKA_CLUSTER', '10.100.198.220:9092'),
    backend=os.environ.get('BACKEND', 'none'),
    retry_interval=os.environ.get('RETRY_INTERVAL', 2),
    heartbeat_topic=os.environ.get('HEARTBEAT_TOPIC', "adapters.heartbeat"),
    probe=os.environ.get('PROBE', ':8080'),
    log_level=os.environ.get('LOG_LEVEL', 'WARN')
)


def parse_args():
    parser = argparse.ArgumentParser()

    _help = ('Path to openonu.yml config file (default: %s). '
             'If relative, it is relative to main.py of openonu adapter.'
             % defs['config'])
    parser.add_argument('-c', '--config',
                        dest='config',
                        action='store',
                        default=defs['config'],
                        help=_help)

    _help = 'Regular expression for extracting conatiner number from ' \
            'container name (default: %s)' % defs['container_name_regex']
    parser.add_argument('-X', '--container-number-extractor',
                        dest='container_name_regex',
                        action='store',
                        default=defs['container_name_regex'],
                        help=_help)

    _help = '<hostname>:<port> to consul agent (default: %s)' % defs['consul']
    parser.add_argument('-C', '--consul',
                        dest='consul',
                        action='store',
                        default=defs['consul'],
                        help=_help)

    _help = 'name of this adapter (default: %s)' % defs['name']
    parser.add_argument('-na', '--name',
                        dest='name',
                        action='store',
                        default=defs['name'],
                        help=_help)

    _help = 'vendor of this adapter (default: %s)' % defs['vendor']
    parser.add_argument('-ven', '--vendor',
                        dest='vendor',
                        action='store',
                        default=defs['vendor'],
                        help=_help)

    _help = 'supported device type of this adapter (default: %s)' % defs[
        'device_type']
    parser.add_argument('-dt', '--device_type',
                        dest='device_type',
                        action='store',
                        default=defs['device_type'],
                        help=_help)

    _help = 'specifies whether the device type accepts bulk flow updates ' \
            'adapter (default: %s)' % defs['accept_bulk_flow']
    parser.add_argument('-abf', '--accept_bulk_flow',
                        dest='accept_bulk_flow',
                        action='store',
                        default=defs['accept_bulk_flow'],
                        help=_help)

    _help = 'specifies whether the device type accepts add/remove flow ' \
            '(default: %s)' % defs['accept_atomic_flow']
    parser.add_argument('-aaf', '--accept_atomic_flow',
                        dest='accept_atomic_flow',
                        action='store',
                        default=defs['accept_atomic_flow'],
                        help=_help)

    _help = '<hostname>:<port> to etcd server (default: %s)' % defs['etcd']
    parser.add_argument('-e', '--etcd',
                        dest='etcd',
                        action='store',
                        default=defs['etcd'],
                        help=_help)

    _help = ('unique string id of this container instance (default: %s)'
             % defs['instance_id'])
    parser.add_argument('-i', '--instance-id',
                        dest='instance_id',
                        action='store',
                        default=defs['instance_id'],
                        help=_help)

    _help = 'ETH interface to recieve (default: %s)' % defs['interface']
    parser.add_argument('-I', '--interface',
                        dest='interface',
                        action='store',
                        default=defs['interface'],
                        help=_help)

    _help = 'omit startup banner log lines'
    parser.add_argument('-n', '--no-banner',
                        dest='no_banner',
                        action='store_true',
                        default=False,
                        help=_help)

    _help = 'do not emit periodic heartbeat log messages'
    parser.add_argument('-N', '--no-heartbeat',
                        dest='no_heartbeat',
                        action='store_true',
                        default=False,
                        help=_help)

    _help = 'enable logging'
    parser.add_argument('-l', '--log_level',
                        dest='log_level',
                        action='store',
                        default=defs['log_level'],
                        help=_help)

    _help = ('use docker container name as conatiner instance id'
             ' (overrides -i/--instance-id option)')
    parser.add_argument('--instance-id-is-container-name',
                        dest='instance_id_is_container_name',
                        action='store_true',
                        default=False,
                        help=_help)

    _help = ('<hostname>:<port> of the kafka adapter broker (default: %s). ('
             'If not '
             'specified (None), the address from the config file is used'
             % defs['kafka_adapter'])
    parser.add_argument('-KA', '--kafka_adapter',
                        dest='kafka_adapter',
                        action='store',
                        default=defs['kafka_adapter'],
                        help=_help)

    _help = ('<hostname>:<port> of the kafka cluster broker (default: %s). ('
             'If not '
             'specified (None), the address from the config file is used'
             % defs['kafka_cluster'])
    parser.add_argument('-KC', '--kafka_cluster',
                        dest='kafka_cluster',
                        action='store',
                        default=defs['kafka_cluster'],
                        help=_help)

    _help = 'backend to use for config persitence'
    parser.add_argument('-b', '--backend',
                        default=defs['backend'],
                        choices=['none', 'consul', 'etcd'],
                        help=_help)

    _help = 'topic of core on the kafka bus'
    parser.add_argument('-ct', '--core_topic',
                        dest='core_topic',
                        action='store',
                        default=defs['core_topic'],
                        help=_help)

    _help = 'topic of events on the kafka bus'
    parser.add_argument('-et', '--event_topic',
                        dest='event_topic',
                        action='store',
                        default=defs['event_topic'],
                        help=_help)

    _help = '<hostname>:<port> for liveness and readiness probes (default: %s)' % defs['probe']
    parser.add_argument(
        '-P', '--probe', dest='probe', action='store',
        default=defs['probe'],
        help=_help)

    args = parser.parse_args()

    # post-processing

    if args.instance_id_is_container_name:
        args.instance_id = get_my_containers_name()

    return args


def load_config(args):
    path = args.config
    if path.startswith('.'):
        dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(dir, path)
    path = os.path.abspath(path)
    with open(path) as fd:
        config = yaml.load(fd)
    return config


def get_build_info():
    path = defs['build_info_file']
    if not path.startswith('/'):
        dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(dir, path)
    path = os.path.abspath(path)
    build_info = configparser.ConfigParser()
    build_info.read(path)
    results = types.SimpleNamespace(
        version=build_info.get('buildinfo', 'version', fallback='unknown'),
        vcs_ref=build_info.get('buildinfo', 'vcs_ref', fallback='unknown'),
        vcs_dirty=build_info.get('buildinfo', 'vcs_dirty', fallback='unknown'),
        build_time=build_info.get('buildinfo', 'build_time', fallback='unknown')
    )
    return results


def print_banner(log):
    log.info('       ___________ _____ _   _ _____ _   _ _   _       ')
    log.info('      |  _  | ___ \  ___| \ | |  _  | \ | | | | |      ')
    log.info('      | | | | |_/ / |__ |  \| | | | |  \| | | | |      ')
    log.info('      | | | |  __/|  __|| . ` | | | | . ` | | | |      ')
    log.info('      \ \_/ / |   | |___| |\  \ \_/ / |\  | |_| |      ')
    log.info('       \___/\_|   \____/\_| \_/\___/\_| \_/\___/       ')
    log.info('                                                       ')


@implementer(IComponent)
class Main(object):

    def __init__(self):

        self.args = args = parse_args()
        self.config = load_config(args)

        # log levels in python are:
        # 1 - DEBUG => verbosity_adjust = 0
        # 2 - INFO => verbosity_adjust = 1
        # 3 - WARNING => verbosity_adjust = 2
        # 4 - ERROR
        # 5 - CRITICAL
        # If no flags are set we want to stick with INFO,
        # if verbose is set we want to go down to DEBUG
        # if quiet is set we want to go up to WARNING
        # if you set both, you're doing something non-sense and you'll be back at INFO

        verbosity_adjust = self.string_to_int(str(args.log_level))
        if verbosity_adjust == -1:
            print("Invalid loglevel is given: " + str(args.log_level))
            sys.exit(0)

        self.log = setup_logging(self.config.get('logging', {}),
                                 args.instance_id,
                                 verbosity_adjust=verbosity_adjust)
        self.log.info('container-number-extractor',
                      regex=args.container_name_regex)

        self.build_info = get_build_info()
        self.log.info('OpenONU-Adapter-Version', build_version=self.build_info)

        if not args.no_banner:
            print_banner(self.log)

        self.adapter = None
        # Create a unique instance id using the passed-in instance id and
        # UTC timestamp
        current_time = arrow.utcnow().timestamp
        self.instance_id = self.args.instance_id + '_' + str(current_time)

        self.core_topic = str(args.core_topic)
        self.event_topic = str(args.event_topic)
        self.listening_topic = str(args.name)
        self.startup_components()

        if not args.no_heartbeat:
            self.start_kafka_cluster_heartbeat(self.instance_id)

    def start(self):
        self.start_reactor()  # will not return except Keyboard interrupt

    def string_to_int(self, loglevel):
        l = loglevel.upper()
        if l == "DEBUG": return 0
        elif l == "INFO": return 1
        elif l == "WARN": return 2
        elif l == "ERROR": return 3
        elif l == "CRITICAL": return 4
        else: return -1

    def stop(self):
        pass

    def get_args(self):
        """Allow access to command line args"""
        return self.args

    def get_config(self):
        """Allow access to content of config file"""
        return self.config

    def _get_adapter_config(self):
        cfg = AdapterConfig()
        return cfg

    @inlineCallbacks
    def startup_components(self):
        try:
            self.log.info('starting-internal-components',
                          consul=self.args.consul,
                          etcd=self.args.etcd)

            registry.register('main', self)

            # Update the logger to output the vcore id.
            self.log = update_logging(instance_id=self.instance_id,
                                      vcore_id=None)

            yield registry.register(
                'kafka_cluster_proxy',
                KafkaProxy(
                    self.args.consul,
                    self.args.kafka_cluster,
                    config=self.config.get('kafka-cluster-proxy', {})
                )
            ).start()
            Probe.kafka_cluster_proxy_running = True
            Probe.kafka_proxy_faulty = False

            config = self._get_adapter_config()

            self.core_proxy = CoreProxy(
                kafka_proxy=None,
                default_core_topic=self.core_topic,
                default_event_topic=self.event_topic,
                my_listening_topic=self.listening_topic)

            self.adapter_proxy = AdapterProxy(
                kafka_proxy=None,
                core_topic=self.core_topic,
                my_listening_topic=self.listening_topic)

            self.adapter = BrcmOpenomciOnuAdapter(
                core_proxy=self.core_proxy, adapter_proxy=self.adapter_proxy,
                config=config,
                build_info=self.build_info)

            self.adapter.start()

            openonu_request_handler = AdapterRequestFacade(adapter=self.adapter,
                                                           core_proxy=self.core_proxy)

            yield registry.register(
                'kafka_adapter_proxy',
                IKafkaMessagingProxy(
                    kafka_host_port=self.args.kafka_adapter,
                    # TODO: Add KV Store object reference
                    kv_store=self.args.backend,
                    default_topic=self.args.name,
                    group_id_prefix=self.args.instance_id,
                    target_cls=openonu_request_handler
                )
            ).start()
            Probe.kafka_adapter_proxy_running = True

            self.core_proxy.kafka_proxy = get_messaging_proxy()
            self.adapter_proxy.kafka_proxy = get_messaging_proxy()

            # retry for ever
            res = yield self._register_with_core(-1)
            Probe.adapter_registered_with_core = True

            self.log.info('started-internal-services')

        except Exception as e:
            self.log.exception('Failure-to-start-all-components', e=e)

    @inlineCallbacks
    def shutdown_components(self):
        """Execute before the reactor is shut down"""
        self.log.info('exiting-on-keyboard-interrupt')
        for component in reversed(registry.iterate()):
            yield component.stop()

        self.server.shutdown()

        import threading
        self.log.info('THREADS:')
        main_thread = threading.current_thread()
        for t in threading.enumerate():
            if t is main_thread:
                continue
            if not t.isDaemon():
                continue
            self.log.info('joining thread {} {}'.format(
                t.getName(), "daemon" if t.isDaemon() else "not-daemon"))
            t.join()

    def start_reactor(self):
        from twisted.internet import reactor
        reactor.callWhenRunning(
            lambda: self.log.info('twisted-reactor-started'))
        reactor.addSystemEventTrigger('before', 'shutdown',
                                      self.shutdown_components)
        reactor.callInThread(self.start_probe)
        reactor.run()

    def start_probe(self):
        args = self.args
        host = args.probe.split(':')[0]
        port = args.probe.split(':')[1]
        socketserver.TCPServer.allow_reuse_address = True
        self.server = socketserver.TCPServer((host, int(port)), Probe)
        self.server.serve_forever()

    @inlineCallbacks
    def _register_with_core(self, retries):
        while 1:
            try:
                resp = yield self.core_proxy.register(
                    self.adapter.adapter_descriptor(),
                    self.adapter.device_types())
                if resp:
                    self.log.info('registered-with-core',
                                  coreId=resp.instance_id)

                returnValue(resp)
            except TimeOutError as e:
                self.log.warn("timeout-when-registering-with-core", e=e)
                if retries == 0:
                    self.log.exception("no-more-retries", e=e)
                    raise
                else:
                    retries = retries if retries < 0 else retries - 1
                    yield asleep(defs['retry_interval'])
            except Exception as e:
                self.log.exception("failed-registration", e=e)
                raise

    # Temporary function to send a heartbeat message to the external kafka
    # broker
    def start_kafka_cluster_heartbeat(self, instance_id):
        # For heartbeat we will send a message to a specific "voltha-heartbeat"
        #  topic.  The message is a protocol buf
        # message
        message = dict(
            type='heartbeat',
            adapter=self.args.name,
            instance=instance_id,
            ip=get_my_primary_local_ipv4()
        )
        topic = defs['heartbeat_topic']

        def send_heartbeat_msg():
            try:
                kafka_cluster_proxy = get_kafka_proxy()
                if kafka_cluster_proxy:
                    message['ts'] = arrow.utcnow().timestamp
                    self.log.debug('sending-kafka-heartbeat-message')

                    # Creating a handler to receive the message callbacks
                    df = Deferred()
                    df.addCallback(self.process_kafka_alive_state_update)
                    kafka_cluster_proxy.register_alive_state_update(df)
                    kafka_cluster_proxy.send_heartbeat_message(topic, dumps(message))
                else:
                    Probe.kafka_cluster_proxy_running = False
                    self.log.error('kafka-proxy-unavailable')
            except Exception as e:
                self.log.exception('failed-sending-message-heartbeat', e=e)

        def check_heartbeat_delivery():
            try:
                kafka_cluster_proxy = get_kafka_proxy()
                if kafka_cluster_proxy:
                    kafka_cluster_proxy.check_heartbeat_delivery()
            except Exception as e:
                self.log.exception('failed-checking-heartbeat-delivery', e=e)

        def schedule_periodic_heartbeat():
            try:
                # Sending the heartbeat message in a loop
                lc_heartbeat = LoopingCall(send_heartbeat_msg)
                lc_heartbeat.start(10)
                # Polling the delivery status more frequently to get early notification
                lc_poll = LoopingCall(check_heartbeat_delivery)
                lc_poll.start(2)
            except Exception as e:
                self.log.exception('failed-kafka-heartbeat-startup', e=e)

        from twisted.internet import reactor
        # Delaying heartbeat initially to let kafka connection be established
        reactor.callLater(5, schedule_periodic_heartbeat)

    # Receiving the callback and updating the probe accordingly
    def process_kafka_alive_state_update(self, alive_state):
        self.log.debug('process-kafka-alive-state-update', alive_state=alive_state)
        Probe.kafka_cluster_proxy_running = alive_state

        kafka_cluster_proxy = get_kafka_proxy()
        if kafka_cluster_proxy:
            Probe.kafka_proxy_faulty = kafka_cluster_proxy.is_faulty()

if __name__ == '__main__':
    Main().start()
