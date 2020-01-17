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
from twisted.internet import reactor
import structlog


class BrcmOpenomciStartupQueue(object):

    def __init__(self, queue_size=10, wait_interval=10):
        self.log = structlog.get_logger()
        self._activating = dict()
        self._queue_size = queue_size
        self._wait_interval = wait_interval
        self.log.debug('setup-queue', queue_size=self._queue_size, wait_interval=self._wait_interval)

    def add_to_queue(self, device_id, openomci_device, core_onu_device):
        self.log.debug('adding-device-to-queue', device_id=device_id, current_queue_size=len(self._activating))

        if len(self._activating) < self._queue_size:
            data = {
                'openomci_device': openomci_device,
                'core_onu_device': core_onu_device
            }
            self.log.debug('starting-device', device_id=device_id, current_queue_size=len(self._activating))
            self._activating[device_id] = data
            reactor.callLater(1, openomci_device.start, core_onu_device)
        else:
            self.log.debug('waiting-device', device_id=device_id, current_queue_size=len(self._activating))
            reactor.callLater(self._wait_interval, self.add_to_queue, device_id, openomci_device, core_onu_device)

    def remove_from_queue(self, device_id):
        self.log.debug('removing-device-from-queue', device_id=device_id, current_queue_size=len(self._activating))
        del self._activating[device_id]
