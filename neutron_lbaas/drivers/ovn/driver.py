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

from oslo_config import cfg
from oslo_log import log as logging
from oslo_serialization import jsonutils
from oslo_service import service
from oslo_utils import excutils
from ovsdbapp.backend.ovs_idl import connection
from ovsdbapp.schema.ovn_northbound import impl_idl

from neutron_lbaas.drivers import driver_base
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
    return connection.Connection(nbidl, cfg.CONF.ovn.ovsdb_connection_timeout)


class OVNDriver(driver_base.LoadBalancerBaseDriver):
    def __init__(self, plugin):
        super(OVNDriver, self).__init__(plugin)
        self.load_balancer = LoadBalancerManager(self)
        self.listener = ListenerManager(self)
        self.pool = PoolManager(self)
        self.member = MemberManager(self)
        self.health_monitor = HealthMonitorManager(self)

        self.ovn_native_protocols = [constants.PROTOCOL_TCP]
        self._nb_ovn_idl = None

    @property
    def ovn_nbdb_api(self):
        if self._nb_ovn_idl is None:
            mm = self.plugin.db._core_plugin.mechanism_manager
            ovn_mech_driver = mm.mech_drivers['ovn'].obj
            self._nb_ovn_idl = ovn_mech_driver._nb_ovn

        return self._nb_ovn_idl

    def find_ovn_lb(self, listener_id):
        find_condition = ('name', '=', listener_id)
        ovn_lb = self.ovn_nbdb_api.db_find(
            'Load_Balancer', find_condition, row=True).execute(
                check_error=True)

        return ovn_lb[0] if len(ovn_lb) == 1 else None


class LoadBalancerManager(driver_base.BaseLoadBalancerManager):

    def create(self, context, lb):
        self.successful_completion(context, lb)

    def update(self, context, old_lb, lb):
        self.successful_completion(context, lb)

    def delete(self, context, lb):
        self.successful_completion(context, lb, delete=True)

    def refresh(self, context, lb):
        pass

    def stats(self, context, lb):
        return {}  # todo


class PoolManager(driver_base.BasePoolManager):

    def create(self, context, pool):
        if pool.listener.protocol not in self.driver.ovn_native_protocols:
            self.failed_completion(context, listener)
        else:
            self.successful_completion(context, pool)

    def update(self, context, old_pool, pool):
        if pool.listener.protocol not in self.driver.ovn_native_protocols:
            self.failed_completion(context, listener)
        else:
            self.successful_completion(context, pool)

    def delete(self, context, pool):
        self.successful_completion(context, pool, delete=True)


class HealthMonitorManager(driver_base.BaseHealthMonitorManager):

    def create(self, context, listener):
        self.successful_completion(context, listener)

    def update(self, context, old_listener, listener):
        self.successful_completion(context, listener)

    def delete(self, context, listener):
        self.successful_completion(context, listener, delete=True)


class ListenerManager(driver_base.BaseListenerManager):

    def create(self, context, listener):
        if listener.protocol not in self.driver.ovn_native_protocols:
            self.failed_completion(context, listener)
            return

        vip = listener.loadbalancer.vip_address
        vip += ':' + str(listener.protocol_port)

        ips = []
        ovn_lb = self.driver.ovn_nbdb_api.lb_add(
            listener.id, vip, ips, may_exist=True, name=listener.id).execute(
                check_error=True)

        lb_subnet = self.driver.plugin.db._core_plugin.get_subnet(
            context, listener.loadbalancer.vip_subnet_id)
        ls_name = "neutron-" + lb_subnet['network_id']
        ovn_ls = self.driver.ovn_nbdb_api.ls_get(
            ls_name).execute(check_error=True)
        self.driver.ovn_nbdb_api.ls_lb_add(
            ovn_ls.uuid, ovn_lb.uuid, may_exist=True).execute(check_error=True)
        self.successful_completion(context, listener)
        
    def update(self, context, old_listener, listener):
        if listener.protocol not in self.driver.ovn_native_protocols:
            self.failed_completion(context, listener)
        else:
            self.successful_completion(context, listener)

    def delete(self, context, listener):
        if listener.protocol not in self.driver.ovn_native_protocols:
            self.successful_completion(context, listener, delete=True)
            return
    
        ovn_lb = self.driver.find_ovn_lb(listener.id)
        if not ovn_lb:
            # Raise an exception
            self.failed_completion(context, listener)
            return

        lb_subnet = self.driver.plugin.db._core_plugin.get_subnet(
            context, listener.loadbalancer.vip_subnet_id)
        ls_name = "neutron-" + lb_subnet['network_id']
        ovn_ls = self.driver.ovn_nbdb_api.ls_get(
            ls_name).execute(check_error=True)
        self.driver.ovn_nbdb_api.ls_lb_del(
            ovn_ls.uuid, ovn_lb.uuid, if_exists=True).execute(check_error=True)
        ovn_lb = self.driver.ovn_nbdb_api.lb_del(
            ovn_lb.uuid).execute(check_error=True)
        self.successful_completion(context, listener, delete=True)


class MemberManager(driver_base.BaseMemberManager):

    def _get_s_ids_of_other_members(self, member):
        return [m.subnet_id for m in member.pool.members if m.id != member.id]

    def create(self, context, member):
        if member.pool.protocol not in self.driver.ovn_native_protocols:
            self.failed_completion(context, member)
            return

        lb_subnet = self.driver.plugin.db._core_plugin.get_subnet(
            context, member.pool.loadbalancer.vip_subnet_id)

        vip = member.pool.loadbalancer.vip_address
        vip += ':' + str(member.pool.listener.protocol_port)

        ip = member.address + ':' + str(member.protocol_port)

        ovn_lb = self.driver.find_ovn_lb(member.pool.listener.id)

        if not ovn_lb:
            # Raise an exception
            self.failed_completion(context, member)
            return

        vip_listener_ips = ovn_lb.vips[vip]
        if vip_listener_ips:
            vip_listener_ips = vip_listener_ips.split(',')
        else:
            vip_listener_ips = []

        vip_listener_ips.append(ip)

        ovn_lb = self.driver.ovn_nbdb_api.lb_add(
            ovn_lb.name, vip, vip_listener_ips, may_exist=True,
            name=member.pool.listener.id).execute(check_error=True)

        if member.subnet_id not in self._get_s_ids_of_other_members(member):
            lb_subnet = self.driver.plugin.db._core_plugin.get_subnet(
                context, member.subnet_id)
            ls_name = "neutron-" + lb_subnet['network_id']
            ovn_ls = self.driver.ovn_nbdb_api.ls_get(
                ls_name).execute(check_error=True)
            self.driver.ovn_nbdb_api.ls_lb_add(
                ovn_ls.uuid, ovn_lb.uuid, may_exist=True).execute(
                    check_error=True)
        self.successful_completion(context, member)

    def update(self, context, old_member, member):
        if member.pool.protocol not in self.driver.ovn_native_protocols:
            self.failed_completion(context, member)
        else:
            self.successful_completion(context, member)

    def delete(self, context, member):
        if member.pool.protocol not in self.driver.ovn_native_protocols:
            self.successful_completion(context, member, delete=True)
            return

        vip = member.pool.loadbalancer.vip_address
        vip += ':' + str(member.pool.listener.protocol_port)

        ip = member.address + ':' + str(member.protocol_port)
        
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
            ovn_lb.name, vip, vip_listener_ips, may_exist=True,
            name=member.pool.listener.id).execute(check_error=True)

        if member.subnet_id not in self._get_s_ids_of_other_members(member):
            lb_subnet = self.driver.plugin.db._core_plugin.get_subnet(
                context, member.subnet_id)
            ls_name = "neutron-" + lb_subnet['network_id']
            ovn_ls = self.driver.ovn_nbdb_api.ls_get(
                ls_name).execute(check_error=True)
            self.driver.ovn_nbdb_api.ls_lb_del(
                ovn_ls.uuid, ovn_lb.uuid, if_exists=True).execute(
                    check_error=True)

        self.successful_completion(context, member, delete=True)
