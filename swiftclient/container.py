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
Container interface.
"""

from swiftclient import base
from swiftclient import swiftobject


class Container(base.Resource):
    """Object representing a swift container.

    Properties:
        * name: name of this container
        * bytes: size in bytes
        * objects: dictionary object name: object"""

    def __init__(self, *args, **kwargs):
        super(Container, self).__init__(*args, **kwargs)
        self.objects={}

    def __repr__(self):
        return "<Container: %s>" % self.name

    def delete(self, delete_objects_first=False):
        """Delete this container."""
        self.maybe_get()
        self.manager.delete(self, delete_objects_first)

    def __iter__(self):
        """Behave like a list for iteration.
        returns an iterator over the list of objects in the container"""
        self.maybe_get()
        return self.objects.values().__iter__()

    def __len__(self):
        return len(self.objects)

    def __getitem__(self,key):
        """Dict-like behaviour for getitem"""
        self.maybe_get()
        return self.objects[key]

    def __delitem__(self,key):
        """Dict-like behaviour for delitem"""
        self.maybe_get()
        self.objects[key].delete()
        self.get()

    def list(self, prefix_match=None):
        self.maybe_get()
        if prefix_match:
            return [self.objects[key] for key in self.objects.keys()
                if key.startswith(prefix_match) ]
        else:
            return self.objects.items()


class ContainerManager(base.Manager):
    """
    Manage :class:`container` resources.
    """
    resource_class = Container

    def create(self, container_name):
        """
        Create a container.

        :param container_name: Name of the new container 
        :rtype: :class:`Container`
        """

        self.connection.put_container(container_name)
        return self.get(container_name)

    def list(self, prefix_match=None):
        """List containers."""
        headers, containers = self.connection.get_account(prefix=prefix_match)
        return [self.get(container['name']) for container in containers]

    def get(self, container):
        """Get container object from container name.
        If a container object is passed in instead of a container name,
        it is returned unaltered."""

        if isinstance(container, basestring):
            container = Container(self,{'name': container})
        return container

    def _get(self, container):
        """Retrieve container metadata from container name

        :param container_name: The name of the container to get.
        :rtype: :class:`Container`
        """

        headers, objects = self.connection.get_container(container.name)

        #NOTE(hughsaunders) create dictionary of obj_name: SwiftObject
        objects = {obj['name']: swiftobject.SwiftObject(self.client.objects,
            {'container': container, 'name': obj['name']}) for obj in objects}

        metadata = {self.mangle_header(k):v for k,v in headers.iteritems()}

        metadata['objects'] = objects
        metadata['name'] = container.name

        return metadata


    def delete(self, container, delete_objects_first=False):
        """
        Delete a container, optionally deleting all objects first.

        :param container: The :class:`container` to delete.
        """

        if delete_objects_first:
            for obj in container:
                obj.delete()

        if len(container):
            raise ValueError("Can't delete non-empty container %s. If you"
                " wish to delete objects first, pass delete_objects_first"
                "=True to container.delete()" % container.name)

        self.connection.delete_container(container.name)
