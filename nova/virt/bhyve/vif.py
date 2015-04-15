# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 Semihalf
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from sets import Set

from nova import utils
from nova.network import freebsd_net as network
from nova import exception
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)

class BhyveVifDriver:
    def __init__(self):
        self._tap_nums = Set() # Set of taken tap numbers
        self._max_tap_num = (2 ** 15) - 1 # FreeBSD limit
        self._start_tap_num = 100
        self._new_tap_num = self._start_tap_num

        self._vifs = {} # Dict of actual vifs served by the driver
                        # in the system {vif[id]: tap device name}

    def plug(self, vif):
        """Create a tap device for the vif and put it on the network's bridge.

        :param vif:
        :return: Name of the tap device created.
        """

        tap_num = self._get_free_tap_number()
        tap_name = 'tap' + str(tap_num)

        network.create_tap_dev(tap_name)

        self._add_iface_to_bridge(tap_name, self._get_bridge_dev(vif))

        # Update internal vifs and taps sets
        self._vifs[vif['id']] = tap_name
        self._tap_nums.add(tap_num)

        #network.plug(vif['network'])

        return tap_name

    def unplug(self, vif):
        """Remove the vif's tap device from the network bridge.

        :param vif:
        :return: The name of the tap device deleted.
        """
        def tap_num(tap_name):
            return int(tap_name[3:])

        tap = self._vifs.get(vif['id'])
        if not tap:
            LOG.warn(_('There is no tap device for vif: %s' % vif['id']))
            return

        network.delete_net_dev(tap)

        # Update internal vifs and taps sets
        del self._vifs[vif['id']]
        self._tap_nums.remove(tap_num(tap))

        return tap

    def tap_name(self, vif):
        return self._vifs.get(vif['id'])

    def _get_free_tap_number(self):
        """Retrieve first free tap device.

        :return: A name of the tap device.
        :raise exception.NovaException:
        """

        def increment(x):
            ret = x + 1
            if ret > self._max_tap_num:
                ret = self._start_tap_num
            return ret

        if len(self._tap_nums) >= self._max_tap_num - self._start_tap_num \
                                     + 1:
            raise exception.NovaException('No free tap devices has left on the '
                                          'system.')

        num = self._new_tap_num
        tries = self._start_tap_num
        for i in range(self._start_tap_num, self._max_tap_num + 1):
            tries += 1
            while num in self._tap_nums:
                num = increment(num)
            
            if not network.device_exists('tap' + str(num)):
                break
            else:
                num = increment(num)

        if tries > self._max_tap_num:
            raise exception.NovaException('No free tap devices has left on the '
                                          'system.')

        self._new_tap_num = increment(num)

        return num

    def _add_iface_to_bridge(self, iface, bridge):
        if not network.device_is_bridge_member(bridge, iface):
            utils.execute('ifconfig', bridge, 'addm', iface, run_as_root=True)

    def _get_bridge_dev(self, vif):
        return vif['network']['bridge']
