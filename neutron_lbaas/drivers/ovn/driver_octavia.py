# Copyright 2017 Red Hat, Inc.
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

from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp.schema.ovn_northbound import impl_idl

from neutron_lbaas.drivers.octavia import driver as octavia_driver
from neutron_lbaas.services.loadbalancer import constants

LOG = logging.getLogger(__name__)
VERSION = "1.0.0"

ovn_opts = [
    cfg.StrOpt('ovn_nb_connection',
               default='tcp:127.0.0.1:6641',
               help=_('The connection string for the OVN_Northbound OVSDB.\n'
                      'Use tcp:IP:PORT for TCP connection.\n'
                      'Use ssl:IP:PORT for SSL connection. The '
                      'ovn_nb_private_key, ovn_nb_certificate and '
                      'ovn_nb_ca_cert are mandatory.\n'
                      'Use unix:FILE for unix domain socket connection.')),
    cfg.StrOpt('ovn_nb_private_key',
               default='',
               help=_('The PEM file with private key for SSL connection to '
                      'OVN-NB-DB')),
    cfg.StrOpt('ovn_nb_certificate',
               default='',
               help=_('The PEM file with certificate that certifies the '
                      'private key specified in ovn_nb_private_key')),
    cfg.StrOpt('ovn_nb_ca_cert',
               default='',
               help=_('The PEM file with CA certificate that OVN should use to'
                      ' verify certificates presented to it by SSL peers')),
    cfg.IntOpt('ovsdb_connection_timeout',
               default=180,
               help=_('Timeout in seconds for the OVSDB '
                      'connection transaction')),
]

cfg.CONF.register_opts(ovn_opts, 'ovn')


def get_ovn_nbdb_connection():
    nbidl = connection.OvsdbIdl.from_server(cfg.CONF.ovn.ovn_nb_connection,
                                            'OVN_Northbound')
    connection.Connection(nbidl, cfg.CONF.ovn.ovsdb_connection_timeout)


class OVNDriver(octavia_driver.OctaviaDriver):
    def __init__(self, plugin):
        super(OVNDriver, self).__init__(plugin)
        self.listener = ListenerManager(self)
        self.pool = PoolManager(self)
        self.member = MemberManager(self)
        self.ovn_native_protocols = [constants.PROTOCOL_TCP]
        self.nbdb_connection = get_ovn_nbdb_connection()
        self.ovn_nbdb_api = impl_idl.OvnNbApiIdlImpl(self.nbdb_connection)

    def find_ovn_lb(self, listener_id):
        find_condition = (('name', '=', listener_id),)
        ovn_lb = self.driver.ovn_nbdb_api.db_find(
            'Load_Balancer', find_condition).execute(check_error=True)

        return ovn_lb

class ListenerManager(octavia_driver.ListenerManager):

    @async_op
    def create(self, context, listener):
        if listener.protocol not in self.driver.ovn_native_protocols:
            super(ListenerManager, self).create(context, listener)
            return

        vip = listener.loadbalancer.vip_address
        vip += ':' + listener.protocol_port

        ips = []
        ovn_lb = self.driver.ovn_nbdb_api.lb_add(vip, ips, may_exist=True,
                name=listener.id).execute(check_error=True)
        
    @async_op
    def update(self, context, old_listener, listener):
        if listener.protocol not in self.driver.ovn_native_protocols:
            super(ListenerManager, self).update(context, old_listener,
                                                listener)
            return

    @async_op
    def delete(self, context, listener):
        if listener.protocol not in self.driver.ovn_native_protocols:
            super(ListenerManager, self).delete(context, listener)
            return
    
        ovn_lb = self.driver.find_ovn_lb(listener.id)
        if not ovn_lb:
            # Raise an exception
            return

        ovn_lb = self.driver.ovn_nbdb_api.lb_del(
            ovn_lb.uuid).execute(check_error=True)

class PoolManager(octavia_driver.PoolManager):

    @async_op
    def create(self, context, pool):
        if pool.protocol not in self.driver.ovn_native_protocols:
            super(PoolManager, self).create(context, pool)
            return

    @async_op
    def update(self, context, old_pool, pool):
        if pool.protocol not in self.driver.ovn_native_protocols:
            super(PoolManager, self).update(context, old_pool, pool)
            return

    @async_op
    def delete(self, context, pool):
        if pool.protocol not in self.driver.ovn_native_protocols:
            super(PoolManager, self).delete(context, pool)
            return


class MemberManager(octavia_driver.MemberManager):

    @async_op
    def create(self, context, member):
        if member.pool.protocol not in self.driver.ovn_native_protocols:
            super(MemberManager, self).create(context, member)
            return

        lb_subnet = self.plugin.db._core_plugin.get_subnet(
            context, member.pool.loadbalancer.vip_subnet_id)

        vip = member.pool.loadbalancer.vip_address
        vip += ':' + member.pool.protocol_port

        ip = member.address + ':' + member.protocol_port

        ovn_lb = self.driver.find_ovn_lb(member.pool.listener.id)

        if not ovn_lb:
            # Raise an exception
            return

        vip_listener_ips = ovn_lb.vips[vip]
        if vip_listener_ips:
            vip_listener_ips = vip_listener_ips.split(',')
        else:
            vip_listener_ips = []

        vip_listener_ips.append(ip)

        ovn_lb = self.driver.ovn_nbdb_api.lb_add(
            vip, vip_listener_ips, may_exist=True,
            name=member.pool.listener.id).execute(check_error=True)
        
        ls_name = "neutron-" + lb_subnet['network_id']
        ovn_ls = self.driver.ovn_nbdb_api.ls_get(
            ls_name).execute(check_error=True)
        self.driver.ovn_nbdb_api.ls_lb_add(
            ovn_ls.uuid, ovn_lb.uuid, may_exist=True).execute(check_error=True)

    @async_op
    def update(self, context, old_member, member):
        if member.pool.protocol not in self.driver.ovn_native_protocols:
            super(MemberManager, self).update(context, old_member, pool)
            return

    @async_op
    def delete(self, context, member):
        if member.pool.protocol not in self.driver.ovn_native_protocols:
            super(MemberManager, self).delete(context, member)
            return

        vip = member.pool.loadbalancer.vip_address
        vip += ':' + member.pool.lprotocol_port

        ip = member.address + ':' + member.protocol_port
        
        ovn_lb = self.driver.find_ovn_lb(member.pool.listener.id)

        if not ovn_lb:
            # Raise an exception
            return

        vip_listener_ips = ovn_lb.vips[vip].split(',')
        if ip not in vip_listener_ips:
            # Raise an exception
            return

        vip_listener_ips.remove(ip)

        ovn_lb = self.driver.ovn_nbdb_api.lb_add(
            vip, vip_listener_ips, may_exist=True,
            name=member.pool.listener.id).execute(check_error=True)
