#   Copyright 2012-2013 OpenStack, LLC.
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may
#   not use this file except in compliance with the License. You may obtain
#   a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.
#

"""Volume v1 Container action implementations"""

import logging

from cliff import command
from cliff import lister
from cliff import show

from openstackclient.common import utils


class CreateContainer(command.Command):
    """Create container command"""

    api = 'object'
    log = logging.getLogger(__name__ + '.CreateContainer')

    def get_parser(self, prog_name):
        parser = super(CreateContainer, self).get_parser(prog_name)
        parser.add_argument(
            'name',
            metavar='<container>',
            help='The name of the container to create',
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)' % parsed_args)
        object_client = self.app.client_manager.object
        object_client.put_container(parsed_args.name)
	container_headers, container = object_client.get_container(
		parsed_args.name
	)
	return

class DeleteContainer(command.Command):
    """Delete container command"""

    api = 'object'
    log = logging.getLogger(__name__ + '.DeleteContainer')

    def get_parser(self, prog_name):
        parser = super(DeleteContainer, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Name of container to delete',
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)' % parsed_args)
        object_client = self.app.client_manager.object
	object_client.delete_container(parsed_args.container)
        return


class ListContainer(lister.Lister):
    """List container command"""

    api = 'object'
    log = logging.getLogger(__name__ + '.ListContainer')
    
    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)' % parsed_args)
	columns = ['Container Name','Object Count','Size (Bytes)']
	return columns, ((x['name'],x['count'],x['bytes']) 
		for x in self.app.client_manager.object.get_account()[1])


class ShowContainer(lister.Lister):
    """Show objects within a container"""

    api = 'object'
    log = logging.getLogger(__name__ + '.ShowContainer')

    def get_parser(self, prog_name):
        parser = super(ShowContainer, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Name of container to show the contents of.')
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)' % parsed_args)
	columns = ['Object Name',
		   'Size (Bytes)',
		   'Last Modified',
                   'Hash']
	return columns, ((x['name'],
			  x['bytes'],
                          x['last_modified'],
 			  x['hash']) 
		for x in self.app.client_manager.object.get_container(parsed_args.container)[1])
