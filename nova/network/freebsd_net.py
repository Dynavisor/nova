# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 Semihalf
# Copyright 2014 Dynavisor (author: Jonathan Wong)
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

"""Implements FreeBSD networking."""

import netaddr
import os

from oslo.config import cfg

from nova import exception
from nova import utils
from nova.openstack.common.gettextutils import _
from nova.openstack.common import importutils
from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging
from nova.openstack.common import processutils
from nova.openstack.common import excutils


# Import routines from linux_net that can be reused
# TODO(md): Should create separate file with common routines for Linux/FreeBSD
from linux_net import get_dhcp_hosts
from linux_net import get_dns_hosts
from linux_net import get_dhcp_opts
from linux_net import get_dhcp_leases
from linux_net import write_to_file
from linux_net import send_arp_for_ip

from linux_net import _execute
from linux_net import _dhcp_file
from linux_net import _dnsmasq_pid_for


LOG = logging.getLogger(__name__)

freebsd_net_opts = [
    cfg.StrOpt('freebsd_net_interface_driver',
               default='nova.network.freebsd_net.FreeBSDBridgeInterfaceDriver',
               help='Driver used to create Ethernet devices.'),
    cfg.StrOpt('fixed_range',
               default='172.24.0.0/16',
               help='Fixed range pool to allow for private communication'),
]
CONF = cfg.CONF
CONF.register_opts(freebsd_net_opts)
CONF.import_opt('host', 'nova.netconf')
CONF.import_opt('use_ipv6', 'nova.netconf')
CONF.import_opt('my_ip', 'nova.netconf')
CONF.import_opt('dhcpbridge', 'nova.network.linux_net')
CONF.import_opt('metadata_host', 'nova.network.linux_net')
CONF.import_opt('metadata_port', 'nova.network.linux_net')
CONF.import_opt('fake_network', 'nova.network.linux_net')
#CONF.import_opt('fixed_range', 'nova.netconf')

interface_driver = None


def _get_interface_driver():
    global interface_driver
    if not interface_driver:
        interface_driver = \
            importutils.import_object(CONF.freebsd_net_interface_driver)
    return interface_driver


def _route_cmd(action, dest, gw):
    """Construct commands to manipulate routes."""
    cmd = ['route', '-q', action, dest, gw]
    return cmd


def _ifconfig_cmd(netif, params=[]):
    """Construct commands to manipulate ifconfig."""
    cmd = ['ifconfig', netif]
    cmd.extend(params)
    return cmd


def _address_to_cidr(address, netmask):
    """Produce a CIDR format address/netmask."""
    out, err = _execute('netmask', '-nc',
                        '%s/%s' % (address, netmask), check_exit_code=0)
    nm = out.strip().split('/')[1]
    return "%s/%s" % (address, nm)


def _route_list(interface):
    """Get list of routes handled by the interface."""
    routes = []
    out, err = _execute('netstat', '-nrW', '-f', 'inet')

    col = 6 # default on freebsd 10.0
    for line in out.split('\n'):
        fields = line.split()
        if "Destination" in line:
            for f in fields:
                if f.strip() == "Netif":
                    col = fields.index(f)
                    break
            
        if len(fields) > 2 and fields[col] == interface:
            if 'G' in fields[2]:
                routes.append(fields)
    
    return routes


def _ip_list(interface):
    """Get list of IP params for the interface."""
    iplist = []
    out, err = _execute(*_ifconfig_cmd(interface))
    for line in out.split('\n'):
        fields = line.split()
        if fields and fields[0] == 'inet':
            iplist.append(fields)
    return iplist


def _delete_ip_from_list(iplist, interface):
    """The list is supposed to be in ifconfig format."""
    out = err = ''
    for fields in iplist:
        # Dynavisor: modified to account for peculiarities with ifconfig output
        params = ['inet'] if fields[0] != 'inet' else []
        # Dynavisor 
        params.extend(fields)
        params.extend(['delete'])
        out, err = _execute(*_ifconfig_cmd(interface, params),
                            run_as_root=True, check_exit_code=0)
    return (out, err)


def _add_ip_from_list(iplist, interface):
    """The list is supposed to be in ifconfig format."""
    out = err = ''
    for fields in iplist:
        # Dynavisor: modified to account for peculiarities with ifconfig output
        params = ['inet'] if fields[0] != 'inet' else []
        # Dynavisor 
        params.extend(fields)
        params.extend(['add'])
        out, err = _execute(*_ifconfig_cmd(interface, params),
                            run_as_root=True, check_exit_code=0)
    return (out, err)


def _delete_routes_from_list(routelist):
    """The list is supposed to be in netstat format."""
    out = err = ''
    for fields in routelist:
        dest = fields[0]
        gw = fields[1]
        out, err = _execute(*_route_cmd('delete', dest, gw),
                            run_as_root=True, check_exit_code=0)
    return (out, err)


def _add_routes_from_list(routelist):
    """The list is supposed to be in netstat format."""
    out = err = ''
    for fields in routelist:
        dest = fields[0]
        gw = fields[1]
        out, err = _execute(*_route_cmd('add', dest, gw),
                            run_as_root=True, check_exit_code=0)
    return (out, err)


def device_exists(device):
    """Check if network device exists."""

    _out, err = _execute(*_ifconfig_cmd(device),
                         check_exit_code=False, run_as_root=True)
    return not err


def device_is_bridge_member(bridge, device):
    """Check if network device is already a bridge member."""
    out, err = _execute(*_ifconfig_cmd(bridge))
    rv = False
    for line in out.split('\n'):
        fields = line.split()
        if fields and fields[0] == 'member:' and fields[1] == device:
            rv = True
    return rv


def metadata_forward():
    IPFW.add_forward_rule(100, "fwd 169.254.169.254,8775 tcp from any to 169.254.169.254 80")

def metadata_accept():
    # Dynavisor TODO: Implement
    LOG.debug(_("CALLED"))


def init_host(ip_range):
    # Dynavisor TODO: Implement
    LOG.debug(_("CALLED"))


def ensure_metadata_ip():
    """Sets up local metadata IP."""
    params = ['alias', '169.254.169.254/32']
    _execute(*_ifconfig_cmd('lo0', params),
             run_as_root=True, check_exit_code=0)


def create_tap_dev(dev, mac_address=None, promisc=False):
    """Create a tap device"""

    if not device_exists(dev):
        try:
            name, err = _execute(*_ifconfig_cmd(dev, ['create']),
                               run_as_root=True, check_exit_code=0)

            name = name.rstrip()
            if err and name != dev:
                _execute(*_ifconfig_cmd(name, ['name', dev]), run_as_root=True,
                         check_exit_code=0)
        except processutils.ProcessExecutionError:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed creating device: '%s'"), dev)
            return

        if mac_address:
            _execute(*_ifconfig_cmd(dev, ['ether', mac_address]),
                     run_as_root=True, check_exit_code=0)

        _execute(*_ifconfig_cmd(dev, ['up']), run_as_root=True,
                 check_exit_code=0)

        if promisc:
            _execute(*_ifconfig_cmd(dev, ['promisc']), run_as_root=True,
                     check_exit_code=0)


def delete_net_dev(dev):
    """Delete network device if exists."""
    if device_exists(dev):
        try:
            _execute(*_ifconfig_cmd(dev, ['destroy']),
                     run_as_root=True, check_exit_code=0)
            LOG.debug(_("Network device removed: '%s'"), dev)
        except processutils.ProcessExecutionError:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed removing network device: '%s'"), dev)


def bind_floating_ip(floating_ip, device):
    """Bind ip to public interface."""
    LOG.debug("bind_floating_ip %s" % (floating_ip,))
    _add_ip_from_list([[floating_ip + '/32']], device)

    if CONF.send_arp_for_ha and CONF.send_arp_for_ha_count > 0:
        send_arp_for_ip(floating_ip, device, CONF.send_arp_for_ha_count)


def unbind_floating_ip(floating_ip, device):
    """Unbind a public ip from public interface."""
    _delete_ip_from_list([[floating_ip]], device)



def ensure_vpn_forward(public_ip, port, private_ip):
    # TODO: Implement similarly to Linux iptables
    pass


def plug(network, mac_address, gateway=True):
    return _get_interface_driver().plug(network, mac_address, gateway)


def unplug(network):
    return _get_interface_driver().unplug(network)


def get_dev(network):
    return _get_interface_driver().get_dev(network)


class FreeBSDNetInterfaceDriver(object):
    """Abstract class that defines generic network host API
    for for all FreeBSD interface drivers.
    """

    def plug(self, network, mac_address):
        """Create Ethernet device, return device name."""
        raise NotImplementedError()

    def unplug(self, network):
        """Destroy Ethernet device, return device name."""
        raise NotImplementedError()

    def get_dev(self, network):
        """Get device name."""
        raise NotImplementedError()


class FreeBSDBridgeInterfaceDriver(FreeBSDNetInterfaceDriver):

    def plug(self, network, mac_address, gateway=True):
        vlan = network.get('vlan')
        if vlan is not None:
            # TODO: Implement vlan support
            raise NotImplementedError()
        else:
            iface = CONF.flat_interface or network['bridge_interface']
            FreeBSDBridgeInterfaceDriver.ensure_bridge(network['bridge'],
                                                       iface, network, gateway)

        if CONF.share_dhcp_address:
            # TODO: isolate_dhcp_address(iface, network['dhcp_server'])
            raise NotImplementedError()

        # TODO: iptables_manager.apply()
        return network['bridge']

    def unplug(self, network, gateway=True):
        vlan = network.get('vlan')
        if vlan is not None:
            # TODO: Implement vlan support
            raise NotImplementedError()
        else:
            iface = CONF.flat_interface or network['bridge_interface']
            FreeBSDBridgeInterfaceDriver.remove_bridge(network['bridge'],
                                                       gateway)

        if CONF.share_dhcp_address:
            # TODO: remove_isolate_dhcp_address(iface, network['dhcp_server'])
            raise NotImplementedError()

        # TODO: iptables_manager.apply()
        return self.get_dev(network)

    def get_dev(self, network):
        return network['bridge']

    @staticmethod
    @utils.synchronized('lock_bridge', external=True)
    def ensure_bridge(bridge, interface, net_attrs=None, gateway=True,
                      filtering=True):
        """Create a bridge unless it already exists.

        :param interface: the interface to create the bridge on.
        :param net_attrs: dictionary with  attributes used to create bridge.
        :param gateway: whether or not the bridge is a gateway.
        :param filtering: whether or not to create filters on the bridge.

        The code will attempt to move any ips that already exist on the
        interface onto the bridge and reset the routes if necessary.

        """

        if not device_exists(bridge):
            LOG.debug(_('Starting Bridge %s'), bridge)
            name, err = _execute(*_ifconfig_cmd('bridge', ['create']),
                                 run_as_root=True, check_exit_code=0)
            name = name.rstrip()
            if name != bridge:
                _execute(*_ifconfig_cmd(name, ['name', bridge]),
                         run_as_root=True, check_exit_code=0)
            _execute(*_ifconfig_cmd(bridge, ['up']), run_as_root=True,
                     check_exit_code=0)

        if interface:
            msg = _('Adding interface %(interface)s to bridge %(bridge)s')
            LOG.debug(msg, {'interface': interface, 'bridge': bridge})

            # Add interface to the bridge
            if not device_is_bridge_member(bridge, interface):
                params = ['addm', interface]
                _execute(*_ifconfig_cmd(bridge, params), run_as_root=True,
                         check_exit_code=0)
            else:
                LOG.debug("Interface %s already a member of bridge %s",
                          interface, bridge)

            out, err = _execute(*_ifconfig_cmd(interface, ['up']), run_as_root=True,
                     check_exit_code=0)

            # Find existing routes
            existing_routes = _route_list(interface)
            _delete_routes_from_list(existing_routes)

            # Find existing IP addresses on the i/f
            existing_ips = _ip_list(interface)

            # Move IP addresses from i/f to the bridge
            _delete_ip_from_list(existing_ips, interface)
            _add_ip_from_list(existing_ips, bridge)

            # Re-add routes
            _add_routes_from_list(existing_routes)
            # dynavisor edit end

            if (err):
                msg = _('Failed to add interface: %s') % err
                raise exception.NovaException(msg)

        if filtering:
            # Don't forward traffic unless we were told to be a gateway
            # TODO: Implement filtering (like IptableManager does in Linux)
            pass

    @staticmethod
    @utils.synchronized('lock_bridge', external=True)
    def remove_bridge(bridge, gateway=True, filtering=True):
        """Delete a bridge."""

        if filtering:
            # TODO: Implement filtering (like IptableManager does in Linux)
            pass

        delete_net_dev(bridge)


@utils.synchronized('lock_gateway', external=True)
def initialize_gateway_device(dev, network_ref):
    if not network_ref:
        return

    _execute('sysctl', 'net.inet.ip.forwarding=1', run_as_root=True)

    # NOTE(vish): The ip for dnsmasq has to be the first address on the
    #             bridge for it to respond to reqests properly
    try:
        prefix = network_ref.cidr.prefixlen
    except AttributeError:
        prefix = network_ref['cidr'].rpartition('/')[2]

    gw_ip = '%s/%s' % (network_ref['dhcp_server'], prefix)

    new_ips = [['inet', gw_ip, 'broadcast', network_ref['broadcast']]]
    interface = dev

    # Find existing IPs
    existing_ips = []
    out, err = _execute(*_ifconfig_cmd(interface))
    for line in out.split('\n'):
        fields = line.split()
        if fields and fields[0] == 'inet':
            existing_ips.append(fields)
            this_ip = _address_to_cidr(fields[1], fields[3])
            if this_ip is not gw_ip:
                new_ips.append(fields)

    # TODO: add comment describing overall process...

    # Get the first address from existing list
    first_ip = ''
    if existing_ips:
        first_ip = _address_to_cidr(existing_ips[0][1], existing_ips[0][3])
    if not existing_ips or first_ip is not gw_ip:
        existing_routes = _route_list(interface)

        # Delete existing routes
        LOG.debug('Delete routes: %s', existing_routes)
        _delete_routes_from_list(existing_routes)

        # Delete existing IPs
        LOG.debug('Delete IPs: %s', existing_ips)
        _delete_ip_from_list(existing_ips, interface)

        # Restore IP config
        LOG.debug('NewIPs: %s', new_ips)
        _add_ip_from_list(new_ips, interface)

        # Restore routes
        LOG.debug('Re-add routes: %s', existing_routes)
        _add_routes_from_list(existing_routes)

        if CONF.send_arp_for_ha and CONF.send_arp_for_ha_count > 0:
            send_arp_for_ip(network_ref['dhcp_server'], dev,
                            CONF.send_arp_for_ha_count)
    if CONF.use_ipv6:
        # TODO: Add ipv6 support
        pass


def release_dhcp(dev, address, mac_address):
    # XXX The dhcp_release is currently (2013.12) not available on FreeBSD
    LOG.warning(_('dhcp_release not available, skipping'))


def update_dhcp(context, dev, network_ref):
    conffile = _dhcp_file(dev, 'conf')
    write_to_file(conffile, get_dhcp_hosts(context, network_ref))
    restart_dhcp(context, dev, network_ref)


def update_dns(context, dev, network_ref):
    hostsfile = _dhcp_file(dev, 'hosts')
    write_to_file(hostsfile, get_dns_hosts(context, network_ref))
    restart_dhcp(context, dev, network_ref)


# NOTE(ja): Sending a HUP only reloads the hostfile, so any
#           configuration options (like dchp-range, vlan, ...)
#           aren't reloaded.
@utils.synchronized('dnsmasq_start')
def restart_dhcp(context, dev, network_ref):
    """(Re)starts a dnsmasq server for a given network.

    If a dnsmasq instance is already running then send a HUP
    signal causing it to reload, otherwise spawn a new instance.

    """
    conffile = _dhcp_file(dev, 'conf')
    LOG.debug('CONF %s', conffile)

    if CONF.use_single_default_gateway:
        # NOTE(vish): this will have serious performance implications if we
        #             are not in multi_host mode.
        optsfile = _dhcp_file(dev, 'opts')
        write_to_file(optsfile, get_dhcp_opts(context, network_ref))
        os.chmod(optsfile, 0o644)

    # TODO: _add_dhcp_mangle_rule(dev)

    # Make sure dnsmasq can actually read it (it setuid()s to "nobody")
    os.chmod(conffile, 0o644)

    pid = _dnsmasq_pid_for(dev)

    # if dnsmasq is already running, then tell it to reload
    if pid:
        out, _err = _execute('ps', '-ww', '-o command=', '-p', pid,
                             check_exit_code=False)
        if conffile.split('/')[-1] in out:
            try:
                _execute('kill', '-HUP', pid, run_as_root=True)
                # TODO: _add_dnsmasq_accept_rules(dev) needs iptables or similar
            except Exception as exc:  # pylint: disable=W0703
                LOG.error(_('Hupping dnsmasq threw %s'), exc)
            return
        else:
            LOG.debug(_('Pid %d is stale, relaunching dnsmasq'), pid)

    cmd = ['env',
           'CONFIG_FILE=%s' % jsonutils.dumps(CONF.dhcpbridge_flagfile),
           'NETWORK_ID=%s' % str(network_ref['id']),
           'dnsmasq',
           '--strict-order',
           '--bind-interfaces',
           '--conf-file=%s' % CONF.dnsmasq_config_file,
           '--pid-file=%s' % _dhcp_file(dev, 'pid'),
           '--listen-address=%s' % network_ref['dhcp_server'],
           '--except-interface=lo',
           '--dhcp-range=set:%s,%s,static,%s,%ss' %
                         (network_ref['label'],
                          network_ref['dhcp_start'],
                          network_ref['netmask'],
                          CONF.dhcp_lease_time),
           '--dhcp-lease-max=%s' % len(netaddr.IPNetwork(network_ref['cidr'])),
           '--dhcp-hostsfile=%s' % _dhcp_file(dev, 'conf'),
           '--dhcp-script=%s' % CONF.dhcpbridge,
           '--leasefile-ro']

    # dnsmasq currently gives an error for an empty domain,
    # rather than ignoring.  So only specify it if defined.
    if CONF.dhcp_domain:
        cmd.append('--domain=%s' % CONF.dhcp_domain)

    dns_servers = set(CONF.dns_server)
    if CONF.use_network_dns_servers:
        if network_ref.get('dns1'):
            dns_servers.add(network_ref.get('dns1'))
        if network_ref.get('dns2'):
            dns_servers.add(network_ref.get('dns2'))
    if network_ref['multi_host'] or dns_servers:
        cmd.append('--no-hosts')
    if network_ref['multi_host']:
        cmd.append('--addn-hosts=%s' % _dhcp_file(dev, 'hosts'))
    if dns_servers:
        cmd.append('--no-resolv')
    for dns_server in dns_servers:
        cmd.append('--server=%s' % dns_server)
    if CONF.use_single_default_gateway:
        cmd += ['--dhcp-optsfile=%s' % _dhcp_file(dev, 'opts')]

    _execute(*cmd, run_as_root=True)


# Dynavisor: copied from linux_net.py

def _add_ip(ip, interface):
    params = ['inet']
    params.extend([ip, 'add'])
    out, err = _execute(*_ifconfig_cmd(interface, params),
        run_as_root=True, check_exit_code=0)
    return (out, err)

def _delete_ip(ip, interface):
    params = ['inet']
    params.extend([ip, 'delete'])
    out, err = _execute(*_ifconfig_cmd(interface, params),
        run_as_root=True, check_exit_code=0)
    return (out, err)


def ensure_floating_forward(floating_ip, fixed_ip, device, network):
    """Ensure floating ip forwarding rule."""
    # Fix add proper error checking for this
    _ensure_ipfw_nat()

    # check with IPFW manager that the ip is not already taken
    if IPFW.floating_ip_used(floating_ip):
        raise Exception("Floating ip %s has been already registered in IPFW" % (floating_ip))

    # allocate ip to the bridge
    _add_ip(floating_ip, network['bridge'])

    IPFW.add_floating_ip_rules(floating_ip, fixed_ip, device, network)



def remove_floating_forward(floating_ip, fixed_ip, device, network):
    """Remove forwarding for floating ip."""
    IPFW.delete_floating_ip_rules(floating_ip, fixed_ip, device, network)
    
    # remove ip from bridge



def _ensure_ipfw_nat():
    """ Check that ipfw_nat kernel module is loaded """
    _execute('kldload','ipfw_nat', run_as_root=True, check_exit_code=False)

class IPFWManager(object):
    """ Wrapper for IPFW and IPFW_NAT 

    All rules get placed in set 1 of ipfw

    Inbound rules are placed first and outbound rules are placed in the same order for different floating ips.
    They start from separate numbering areas, this is to allow for flexibility in modifying the firewall rules.

    The manager is reponsible for keeping track of which floating ips are being used for a given rule.

    """

    def __init__(self):
        self.inbound_rule_start = 1000
        self.outbound_rule_start = 3000
        self.max_floating_ips = 1000
        # offset_list tracks rules and nat instancse
        self.offset_list = []
        # maps floating ip to offset
        self.floating_ip_tracker = {} 

    def floating_ip_used(self, floating_ip):
        return floating_ip in self.floating_ip_tracker

    def add_forward_rule(self, ruleno, rule):
        params = ['ipfw', 'add',  "%d" % ruleno]
        params.extend(rule.split(" "))
        utils.execute( *params, run_as_root=True, check_exit_code=0)

    def _add_nat_instance(self, offset, floating_ip, fixed_ip):
        nat_instance = 1 + offset
        params = ['ipfw', 'nat', "%s" % nat_instance, 'config', 'redirect_addr', fixed_ip, floating_ip]
        utils.execute( *params, run_as_root=True, check_exit_code=0)

    def _delete_nat_instance(self, offset):
        nat_instance = 1 + offset
        _execute('ipfw', 'nat', "%s" % nat_instance, 'delete',
                run_as_root=True, check_exit_code=0)

    def _next_available_rule(self,):
        # find next available rule offset
        for offset in range(self.max_floating_ips):
            if not (offset in self.offset_list):
                return offset

        raise Exception("Failed to find suitable space to insert ipfw rule!")


    def _ipfw_add_rule(self, rule_no, nat_no, src='any', dest=['any'], inbound=False, outbound=False):
        params = ['ipfw', 'add', "%s" % rule_no, 'nat', "%s" % nat_no, 'all', 'from', src, 'to']
        params.extend(dest)
        if inbound:
            params.extend(['in'])
        elif outbound:
            params.extend(['out'])

        _execute(*params, run_as_root=True, check_exit_code=0)

    def _ipfw_delete_rule(self, rule_no):
        params = ['ipfw', 'delete', "%s" % rule_no] 
        _execute(*params, run_as_root=True, check_exit_code=0)
       
    def add_floating_ip_rules(self, floating_ip, fixed_ip, device, network):
        # config nat instance first
        offset = self._next_available_rule()

        # configure nat instance first
        self._add_nat_instance(offset, floating_ip, fixed_ip)
        
        # add ip rules
        inbound_rule = offset + self.inbound_rule_start
        outbound_rule = offset + self.outbound_rule_start
        nat_no = offset + 1
        self._ipfw_add_rule(inbound_rule, nat_no, dest=[floating_ip], inbound=True)
        self._ipfw_add_rule(outbound_rule, nat_no, src=fixed_ip, dest=["{","not","%s" % CONF.fixed_range,"}"])
        
        # add offset to offset list
        self.offset_list.append(offset)

        # track floating ip
        self.floating_ip_tracker[floating_ip] = offset
        
    def delete_floating_ip_rules(self, floating_ip, fixed_ip, device, network):
        # find offset of floating_ip
        offset = self.floating_ip_tracker[floating_ip]

        # remove rules first
        inbound_rule = offset + self.inbound_rule_start
        outbound_rule = offset + self.outbound_rule_start
        self._ipfw_delete_rule(inbound_rule)
        self._ipfw_delete_rule(outbound_rule)
        
        # remove nat instance
 	self.offset_list.remove(offset)
        self._delete_nat_instance(offset)

# Start IPFWManager here
IPFW = IPFWManager()

    # Dynavisor TODO: _add_dnsmasq_accept_rules(dev) needs iptables or similar
