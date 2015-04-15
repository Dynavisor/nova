# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 Semihalf
# Copyright Dynavisor 2014, 2015 (author: Jonathan Wong)
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


"""
A bhyve hypervisor's ComputeDriver implementation.
"""

import uuid
import os,re

from nova.virt.bhyve import vif

from oslo.config import cfg

from nova import block_device
from nova.compute import power_state
from nova.compute import task_states
from nova import db
from nova import exception
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.virt import driver
from nova.virt import virtapi
from nova.network import freebsd_net as network_driver

from nova.virt.libvirt import utils as libvirt_utils
from nova.image import glance
from nova.openstack.common import fileutils
from nova.compute import utils as compute_utils
from nova import utils

import nova.virt.images as virt_images
import images
import bhyve

CONF = cfg.CONF
CONF.import_opt('host', 'nova.netconf')

bhyve_opts = [
    cfg.StrOpt('snapshot_image_format',
               help='Snapshot image format (valid options are : '
                    'raw, qcow2, vmdk, vdi). '
                    'Defaults to same as source image',
               deprecated_group='DEFAULT'),
    cfg.StrOpt('snapshots_directory',
               default='$instances_path/snapshots',
               help='Location where libvirt driver will store snapshots '
                    'before uploading them to image service',
               deprecated_name='libvirt_snapshots_directory',
               deprecated_group='DEFAULT'),
]

CONF.register_opts(bhyve_opts, 'bhyve')

# Live snapshot requirements
REQ_HYPERVISOR_LIVESNAPSHOT = "QEMU"
MIN_LIBVIRT_LIVESNAPSHOT_VERSION = (1, 0, 0)
MIN_QEMU_LIVESNAPSHOT_VERSION = (1, 3, 0)

LOG = logging.getLogger(__name__)


_FAKE_NODES = None


def set_nodes(nodes):
    """Sets BhyveDriver's node.list.

    It has effect on the following methods:
        get_available_nodes()
        get_available_resource
        get_host_stats()

    To restore the change, call restore_nodes()
    """
    global _FAKE_NODES
    _FAKE_NODES = nodes


def restore_nodes():
    """Resets BhyveDriver's node list modified by set_nodes().

    Usually called from tearDown().
    """
    global _FAKE_NODES
    _FAKE_NODES = [CONF.host]


class BhyveInstance(object):

    def __init__(self, name, state):
        self.name = name
        self.state = state

    def __getitem__(self, key):
        return getattr(self, key)


class BhyveDriver(driver.ComputeDriver):
    capabilities = {
        "has_imagecache": False,
        "supports_recreate": False,
        }

    """Fake bhyve driver."""

    def __init__(self, virtapi, read_only=False):
        super(BhyveDriver, self).__init__(virtapi)
        self.instances = {}
        self.host_status_base = {
          'vcpus': 1,
          'memory_mb': 1024,
          'local_gb': 512,
          'vcpus_used': 0,
          'memory_mb_used': 1024,
          'local_gb_used': 10,
          'hypervisor_type': 'bhyve',
          'hypervisor_version': '1.0',
          'hypervisor_hostname': CONF.host,
          'cpu_info': {},
          'disk_available_least': 512,
          }
        self._mounts = {}
        self._interfaces = {}
        if not _FAKE_NODES:
            set_nodes([CONF.host])

        self._bhyve = bhyve.Bhyve()
        self._vif_driver = vif.BhyveVifDriver()
        self._vms = {} # {Instance ID: bhyve.Vm object}

    def init_host(self, host):
        return

    def list_instances(self):
        return self.instances.keys()

    def plug_vifs(self, instance, network_info):
        """Plug VIFs into networks."""

        vm = self._vms.get(instance['uuid'])
        if not vm:
            LOG.error(_('Trying to plug VIF to non-existing VM'))
            return

        try:
            for vif in network_info:
                tap = self._vif_driver.plug(vif)
                # Dynavisor TODO: driver type (virtio-net) shouldn't be fixed
                vm.add_net_interface(tap, 'virtio-net', vif['address'])

        except Exception as e:
            LOG.error(_('Failed plugging vif(s) to the network bridge: %s' % e))
            return

    def unplug_vifs(self, instance, network_info):
        """Unplug VIFs from networks."""

        for vif in network_info:
            tap = self._vif_driver.unplug(vif)
            vm = self._vms.get(instance['uuid'])
            if not vm:
                LOG.warn(_('Trying to remove the %s device ' % tap +
                           'associated with non-existing VM id=%s'
                           % instance['uuid']))
                continue
            vm.del_net_interface(tap)

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        name = instance['name']
        state = power_state.RUNNING
        fake_instance = BhyveInstance(name, state)
        self.instances[name] = fake_instance

        image_path = images.fetch(context, instance, image_meta,
                                  injected_files, admin_password)

        vm = bhyve.Vm(self._bhyve, instance['name'], instance['vcpus'],
                      instance['memory_mb'])
        # Save vm object
        self._vms[instance['uuid']] = vm

        # Dynavisor TODO: get rid of hardcoded driver
        # vm.add_disk_image('ahci-hd', image_path, boot=True)
        vm.add_disk_image('virtio-blk', image_path, boot=True)
        # Dynavisor

        self.plug_vifs(instance, network_info)
        vm.run()

    def _create_snapshot_metadata(self, base, instance, img_fmt, snp_name):
        metadata = {'is_public': False,
                    'status': 'active',
                    'name': snp_name,
                    'properties': {
                                   'kernel_id': instance['kernel_id'],
                                   'image_location': 'snapshot',
                                   'image_state': 'available',
                                   'owner_id': instance['project_id'],
                                   'ramdisk_id': instance['ramdisk_id'],
                                   }
                    }
        if instance['os_type']:
            metadata['properties']['os_type'] = instance['os_type']

        # NOTE(vish): glance forces ami disk format to be ami
        if base.get('disk_format') == 'ami':
            metadata['disk_format'] = 'ami'
        else:
            metadata['disk_format'] = img_fmt

        metadata['container_format'] = base.get('container_format', 'bare')

        return metadata

    def snapshot(self, context, instance, image_href, update_task_state):
        """Create snapshot from a running VM instance.

        This command only works with qemu 0.14+
        """
        try:
            virt_dom = self._bhyve.get_vm_by_name(instance['name'])
        except exception.InstanceNotFound:
            raise exception.InstanceNotRunning(instance_id=instance['uuid'])

        (image_service, image_id) = glance.get_remote_image_service(
            context, instance['image_ref'])

        base = compute_utils.get_image_metadata(
            context, image_service, image_id, instance)

        _image_service = glance.get_remote_image_service(context, image_href)
        snapshot_image_service, snapshot_image_id = _image_service
        snapshot = snapshot_image_service.show(context, snapshot_image_id)

        disk_path = images.get_instance_image_path(instance)
        source_format =  libvirt_utils.get_disk_type(disk_path)

        # Dynavisor: only support source_format raw atm
        image_format = source_format

        LOG.info("disk_path = %s, source_format = %s\n" % (disk_path, source_format))

        metadata = self._create_snapshot_metadata(base,
                                                  instance,
                                                  image_format,
                                                  snapshot['name'])

        snapshot_name = uuid.uuid4().hex
        LOG.info("metadata = %s, snapshot_name = %s\n" % (metadata, snapshot_name))

        (state, _max_mem, _mem, _cpus, _t) = self.get_info(instance)

        LOG.info("instance state [%s]\n" % state)

        # Disable live snapshot
        live_snapshot = False

        # NOTE(rmk): We cannot perform live snapshots when a managedSave
        #            file is present, so we will use the cold/legacy method
        #            for instances which are shutdown.
        if state == power_state.SHUTDOWN:
            live_snapshot = False

        if live_snapshot:
            LOG.info(_("Beginning live snapshot process"),
                     instance=instance)
        else:
            LOG.info(_("Beginning cold snapshot process"),
                     instance=instance)

        update_task_state(task_state=task_states.IMAGE_PENDING_UPLOAD)
        snapshot_directory = CONF.bhyve.snapshots_directory
        fileutils.ensure_tree(snapshot_directory)

        # create snapshot using qemu-img and skip new domain creation
        with utils.tempdir(dir=snapshot_directory) as tmpdir:
            try:
                out_path = os.path.join(tmpdir, snapshot_name)
                virt_images.convert_image(disk_path, out_path, image_format)
            finally:
                # NOTE(dkang): because previous managedSave is not called
                #              for LXC, _create_domain must not be called.
                LOG.info(_("Snapshot extracted, beginning image upload"),
                         instance=instance)

            # Upload that image to the image service

            update_task_state(task_state=task_states.IMAGE_UPLOADING,
                     expected_state=task_states.IMAGE_PENDING_UPLOAD)

            with  libvirt_utils.file_open(out_path) as image_file:
                image_service.update(context,
                                     image_href,
                                     metadata,
                                     image_file)
                LOG.info(_("Snapshot image upload complete"),
                         instance=instance)

    def reboot(self, context, instance, network_info, reboot_type,
               block_device_info=None, bad_volumes_callback=None):
        pass

    @staticmethod
    def get_host_ip_addr():
        return '192.168.0.1'

    def set_admin_password(self, instance, new_pass):
        pass

    def inject_file(self, instance, b64_path, b64_contents):
        pass

    def resume_state_on_host_boot(self, context, instance, network_info,
                                  block_device_info=None):
        pass

    def rescue(self, context, instance, network_info, image_meta,
               rescue_password):
        pass

    def unrescue(self, instance, network_info):
        pass

    def poll_rebooting_instances(self, timeout, instances):
        pass

    def migrate_disk_and_power_off(self, context, instance, dest,
                                   instance_type, network_info,
                                   block_device_info=None):
        pass

    def finish_revert_migration(self, context, instance, network_info,
                                block_device_info=None, power_on=True):
        pass

    def post_live_migration_at_destination(self, context, instance,
                                           network_info,
                                           block_migration=False,
                                           block_device_info=None):
        pass

    def power_off(self, instance):
        """Power off the instance.

        Currently it is more 'unplug the cable' than 'power off' as bhyve does
        not provide a way to do it cleanly yet.
        """

        # Destroy the VM if it exists.
        vm = self._bhyve.get_vm_by_name(instance['name'])
        if vm:
            for tap in vm.net_interfaces:
                network_driver.delete_net_dev(tap)
                vm.del_net_interface(tap)

            vm.destroy()

    def power_on(self, context, instance, network_info,
                 block_device_info=None):
        """Power on the instance"""

        vm = self._vms.get(instance['uuid'])
        if not vm:
            LOG.warn(_('Trying to power on uknkown VM instance: %s'
                       % instance['uuid']))
            return

        self.plug_vifs(instance, network_info)
        vm.run()

    def soft_delete(self, instance):
        pass

    def restore(self, instance):
        pass

    def pause(self, instance):
        pass

    def unpause(self, instance):
        pass

    def suspend(self, instance):
        pass

    def resume(self, context, instance, network_info, block_device_info=None):
        pass

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True):
        key = instance['name']
        if key in self.instances:
            del self.instances[key]
        else:
            LOG.warning(_("Key '%(key)s' not in instances '%(inst)s'") %
                        {'key': key,
                         'inst': self.instances}, instance=instance)

        # Remove vm from our vm list.
        vm = self._vms.get(instance['uuid'])
        if vm:
            del self._vms[instance['uuid']]

        # Destroy vm if it is running, saving taps to be destroyed.
        taps = {}
        vm = self._bhyve.get_vm_by_name(instance['name'])
        if vm:
            taps = vm.net_interfaces
            self._bhyve.destroy_vm(vm)

        # Clean up tap devices used by the destroyed vm.
        for vif in network_info:
            if self._vif_driver.tap_name(vif) in taps:
                self._vif_driver.unplug(vif)

        if destroy_disks:
            images.delete_instance_image(instance)

    def attach_volume(self, context, connection_info, instance, mountpoint,
                      encryption=None):
        """Attach the disk to the instance at mountpoint using info."""
        instance_name = instance['name']
        if instance_name not in self._mounts:
            self._mounts[instance_name] = {}
        self._mounts[instance_name][mountpoint] = connection_info
        return True

    def detach_volume(self, connection_info, instance, mountpoint,
                      encryption=None):
        """Detach the disk attached to the instance."""
        try:
            del self._mounts[instance['name']][mountpoint]
        except KeyError:
            pass
        return True

    def swap_volume(self, old_connection_info, new_connection_info,
                    instance, mountpoint):
        """Replace the disk attached to the instance."""
        instance_name = instance['name']
        if instance_name not in self._mounts:
            self._mounts[instance_name] = {}
        self._mounts[instance_name][mountpoint] = new_connection_info
        return True

    def attach_interface(self, instance, image_meta, vif):
        if vif['id'] in self._interfaces:
            raise exception.InterfaceAttachFailed('duplicate')
        self._interfaces[vif['id']] = vif

    def detach_interface(self, instance, vif):
        try:
            del self._interfaces[vif['id']]
        except KeyError:
            raise exception.InterfaceDetachFailed('not attached')

    def get_info(self, instance):
        if instance['name'] not in self.instances:
            raise exception.InstanceNotFound(instance_id=instance['name'])
        i = self.instances[instance['name']]
        return {'state': i.state,
                'max_mem': 0,
                'mem': 0,
                'num_cpu': 2,
                'cpu_time': 0}

    def get_diagnostics(self, instance_name):
        return {'cpu0_time': 17300000000,
                'memory': 524288,
                'vda_errors': -1,
                'vda_read': 262144,
                'vda_read_req': 112,
                'vda_write': 5778432,
                'vda_write_req': 488,
                'vnet1_rx': 2070139,
                'vnet1_rx_drop': 0,
                'vnet1_rx_errors': 0,
                'vnet1_rx_packets': 26701,
                'vnet1_tx': 140208,
                'vnet1_tx_drop': 0,
                'vnet1_tx_errors': 0,
                'vnet1_tx_packets': 662,
        }

    def get_all_bw_counters(self, instances):
        """Return bandwidth usage counters for each interface on each
           running VM.
        """
        bw = []
        return bw

    def get_all_volume_usage(self, context, compute_host_bdms):
        """Return usage info for volumes attached to vms on
           a given host.
        """
        volusage = []
        return volusage

    def block_stats(self, instance_name, disk_id):
        return [0L, 0L, 0L, 0L, None]

    def interface_stats(self, instance_name, iface_id):
        return [0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L]

    def get_console_output(self, instance):
        return 'FAKE CONSOLE OUTPUT\nANOTHER\nLAST LINE'

    def get_vnc_console(self, instance):
        return {'internal_access_path': 'FAKE',
                'host': 'fakevncconsole.com',
                'port': 6969}

    def get_spice_console(self, instance):
        return {'internal_access_path': 'FAKE',
                'host': 'fakespiceconsole.com',
                'port': 6969,
                'tlsPort': 6970}

    def get_console_pool_info(self, console_type):
        return {'address': '127.0.0.1',
                'username': 'fakeuser',
                'password': 'fakepassword'}

    def refresh_security_group_rules(self, security_group_id):
        return True

    def refresh_security_group_members(self, security_group_id):
        return True

    def refresh_instance_security_rules(self, instance):
        return True

    def refresh_provider_fw_rules(self):
        pass

    def _get_fbsd_vcpus(self):
        return int((utils.execute("sysctl","hw.ncpu")[0]).split(":")[-1])

    def _get_fbsd_ram(self):
        return int((utils.execute("sysctl","hw.realmem")[0]).split(":")[-1])/1024/1024

    def _get_fbsd_free_ram(self):
        output = utils.execute("sysctl","vm")[0]
        res = re.search(".*Free Memory:\s+(?P<free>\d+)", output)
        return int(res.group("free"))/1024

    def _get_fbsd_total_gb(self):
        output = utils.execute("df","-g")[0]
        lines = output.split("\n")
        total = 0
        LOG.debug(output)
        for line in lines:
            if len(line) > 0:
                row = line.split()
                if "/dev" in row[0]:
                    diskspace = int(row[1])
                    total += diskspace
        LOG.debug("local_gb: %d" % (total))
        return total
    
    def get_available_resource(self, nodename):
        """Updates compute manager resource info on ComputeNode table.

           Since we don't have a real hypervisor, pretend we have lots of
           disk and ram.
        """
        if nodename not in _FAKE_NODES:
            return {}

        LOG.debug("cpus %d" % int((utils.execute("sysctl","hw.ncpu")[0]).split(":")[-1]))

        dic = {'vcpus': self._get_fbsd_vcpus(),
               'memory_mb': self._get_fbsd_ram(),
               'local_gb': self._get_fbsd_total_gb(),
               'vcpus_used': 2,
               'memory_mb_used': 1024*4,
               'local_gb_used': 1024,
               'hypervisor_type': "bhyve",
               'hypervisor_version': '0.1',
               'hypervisor_hostname': nodename,
               'disk_available_least': 2,
               'cpu_info': '?'}
        return dic

    def ensure_filtering_rules_for_instance(self, instance_ref, network_info):
        return

    def get_instance_disk_info(self, instance_name):
        return

    def live_migration(self, context, instance_ref, dest,
                       post_method, recover_method, block_migration=False,
                       migrate_data=None):
        post_method(context, instance_ref, dest, block_migration,
                            migrate_data)
        return

    def check_can_live_migrate_destination_cleanup(self, ctxt,
                                                   dest_check_data):
        return

    def check_can_live_migrate_destination(self, ctxt, instance_ref,
                                           src_compute_info, dst_compute_info,
                                           block_migration=False,
                                           disk_over_commit=False):
        return {}

    def check_can_live_migrate_source(self, ctxt, instance_ref,
                                      dest_check_data):
        return

    def finish_migration(self, context, migration, instance, disk_info,
                         network_info, image_meta, resize_instance,
                         block_device_info=None, power_on=True):
        return

    def confirm_migration(self, migration, instance, network_info):
        return

    def pre_live_migration(self, context, instance_ref, block_device_info,
                           network_info, disk, migrate_data=None):
        return

    def unfilter_instance(self, instance_ref, network_info):
        return

    def test_remove_vm(self, instance_name):
        """Removes the named VM, as if it crashed. For testing."""
        self.instances.pop(instance_name)

    def get_host_stats(self, refresh=False):
        """Return fake Host Status of ram, disk, network."""
        stats = []
        for nodename in _FAKE_NODES:
            host_status = self.host_status_base.copy()
            host_status['hypervisor_hostname'] = nodename
            host_status['host_hostname'] = nodename
            host_status['host_name_label'] = nodename
            stats.append(host_status)
        if len(stats) == 0:
            raise exception.NovaException("BhyveDriver has no node")
        elif len(stats) == 1:
            return stats[0]
        else:
            return stats

    def host_power_action(self, host, action):
        """Reboots, shuts down or powers up the host."""
        return action

    def host_maintenance_mode(self, host, mode):
        """Start/Stop host maintenance window. On start, it triggers
        guest VMs evacuation.
        """
        if not mode:
            return 'off_maintenance'
        return 'on_maintenance'

    def set_host_enabled(self, host, enabled):
        """Sets the specified host's ability to accept new instances."""
        if enabled:
            return 'enabled'
        return 'disabled'

    def get_disk_available_least(self):
        pass

    def get_volume_connector(self, instance):
        return {'ip': '127.0.0.1', 'initiator': 'fake', 'host': 'fakehost'}

    def get_available_nodes(self, refresh=False):
        return _FAKE_NODES

    def instance_on_disk(self, instance):
        return False

    def list_instance_uuids(self):
        return []

    #def get_available_resource(self, nodename):
    #    """Retrieve resource information.
    #    from libvirt
    #    """
    #    stats = self.get_host_stats(refresh=True)
    #    return stats
    

class FakeVirtAPI(virtapi.VirtAPI):
    def instance_update(self, context, instance_uuid, updates):
        return db.instance_update_and_get_original(context,
                                                   instance_uuid,
                                                   updates)

    def provider_fw_rule_get_all(self, context):
        return db.provider_fw_rule_get_all(context)

    def agent_build_get_by_triple(self, context, hypervisor, os, architecture):
        return db.agent_build_get_by_triple(context,
                                            hypervisor, os, architecture)

    def instance_type_get(self, context, instance_type_id):
        return db.flavor_get(context, instance_type_id)

    def block_device_mapping_get_all_by_instance(self, context, instance,
                                                 legacy=True):
        bdms = db.block_device_mapping_get_all_by_instance(context,
                                                           instance['uuid'])
        if legacy:
            bdms = block_device.legacy_mapping(bdms)
        return bdms

    def block_device_mapping_update(self, context, bdm_id, values):
        return db.block_device_mapping_update(context, bdm_id, values)
