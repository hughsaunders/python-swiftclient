# Copyright 2011 Denali Systems, Inc.
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
object interface.
"""

from swiftclient import base

class SwiftObject(base.Resource):
    """Object representing a swift object.

    Called SwiftObject to avoid shadowing object."""

    def __repr__(self):
        return "<SwiftObject: %s>" % self.name

    def delete(self):
        """Delete this object."""
        self.manager.delete(self)


class SwiftObjectManager(base.Manager):
    """
    Manage :class:`SwiftObject` resources.
    """
    resource_class = SwiftObject

    def get(self, container, object_name):
        """Get SwiftObject that represents a swift object.

        container should be the name of a container or a container.Container
        """
        return self.client.containers.get(container)[object_name]

    def _get(self, swiftobject):
        """Referesh object data from server.

        :param container: Name or Container object
        :param object_name: Name of object to refresh
        :rtype: :class:`SwiftObject`
        """

        headers = self.connection.head_object(swiftobject.container.name,
                                               swiftobject.name)
        metadata = {self.mangle_header(k):v for k,v in headers.iteritems()}
        metadata['name'] = swiftobject.name

        return metadata 

    def list(self, container, prefix_match=None):
        return self.client.containers.get(container).list(prefix_match)

    def delete(self, swiftobject):
        """
        Delete a object.

        :param object: The :class:`object` to delete.
        """
        self.connection.delete_object(swiftobject.container.name,
                                      swiftobject.name)
        swiftobject.container.refresh()

