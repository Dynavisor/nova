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


import os

from oslo.config import cfg

from nova.virt.images import fetch as virt_module_fetch
from nova.openstack.common import log as logging
from nova.openstack.common.gettextutils import _
from nova.virt import images

CONF = cfg.CONF
CONF.import_opt('instances_path', 'nova.compute.manager')
LOG = logging.getLogger(__name__)


def get_instance_image_path(instance):
    return os.path.join(CONF.instances_path, instance['uuid'])


def fetch(context, instance, image_meta, injected_files, admin_password):
    path = os.path.join(CONF.instances_path, instance['uuid'])
    virt_module_fetch(context, instance['image_ref'], path, instance['user_id'],
                      instance['project_id'])

    return path


def delete_instance_image(instance):
    path = os.path.join(CONF.instances_path, instance['uuid'])
    if os.path.exists(path):
        LOG.info(_('Deleting instance image file %s' % path))
        try:
            os.remove(path)
        except OSError as e:
            LOG.error(_('Failed to delete instance image file %s with %s' %
                        (path, e)))

        if os.path.exists(path):
            return False

        LOG.info(_('Deletion of instance image file %s completed.' % path))
        return True

    LOG.info(_('Instance image file %s does not exists.' % path))
    return True