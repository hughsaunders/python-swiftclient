# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import multiprocessing
import Queue
import sys
import threading
import time
import traceback

"""Miscellaneous utility functions for use with Swift."""

TRUE_VALUES = set(('true', '1', 'yes', 'on', 't', 'y'))


def config_true_value(value):
    """
    Returns True if the value is either True or a string in TRUE_VALUES.
    Returns False otherwise.
    This function come from swift.common.utils.config_true_value()
    """
    return value is True or \
        (isinstance(value, basestring) and value.lower() in TRUE_VALUES)


class ThreadManager(object):
    """Manage a pool of threads for running jobs in parallel."""

    class ThreadStopSignal(object):
        """Passed to a thread via input queue to signal that it should quit"""
        pass

    class JobFailureException(Exception):
        """Raised by ThreadManager.join() once processing has finished if
        one or more jobs have failed."""

        def __str__(self):

            def format_exc_info(ei):
                _type, _msg, _traceback = ei
                return "<Exception Type:%s, Message: %s, TraceBack: %s>" %\
                    (_type, _msg, "\n".join(traceback.format_tb(_traceback)))

            return "<JobFailureException: %s>" % ",".join(
                [format_exc_info(ei) for ei in getattr(self, "errors", [])]
            )

    QueueItem = collections.namedtuple('QueueItem', ['priority', 'item'])

    def __init__(self, inputs, function, max_threads=None,
                 queue_size=10000, cpu_multiplier=4, print_queue=None,
                 error_queue=None, output_queue=None):
        """Create a thread manager to process tasks with func.

        :param tasks: iterable containing the objects to be processed
        :param func: function to process each input with
        :param max_threads: maximum number of threads to start
        :param cpu_multiplier: if max_threads is not specified
                               cpu_count*cpu_multiplier will be used instead
        """

        self.max_threads = max_threads
        if not self.max_threads:
            self.max_threads = multiprocessing.cpu_count()

        num_inputs = len(inputs)

        if num_inputs < self.max_threads:
            self.max_threads = num_inputs

        self.queue_size = queue_size
        if num_inputs > self.queue_size:
            self.queue_size = num_inputs

        self.queues = {
            'input': Queue.PriorityQueue(self.queue_size),
            'error': error_queue if error_queue else
            Queue.Queue(self.queue_size),
            'output': output_queue if output_queue else
            Queue.Queue(self.queue_size),
            'print': print_queue if print_queue else
            Queue.Queue(self.queue_size)
        }

        self.threads = []
        self.inputs = inputs
        self.function = function

    def get_jobfunction(self, function):

        def _jobfunction():
            while not self.queues['input'].empty():
                try:
                    input_obj = self.queues['input'].get_nowait().item
                    if isinstance(input_obj, ThreadManager.ThreadStopSignal):
                        break
                except Queue.Empty:
                    break
                except Exception:
                    self.queues['error'].put(sys.exc_info())
                    break

                try:
                    result = function(input_obj, self.queues['print'])
                    if result is not None:
                        self.queues['output'].put(result)
                except Exception:
                    self.queues['error'].put(sys.exc_info())
                finally:
                    # NOTE(hughsaunders) must mark task as done even if item
                    # failed, otherwise queue.join() will block infinitely.
                    # Failure can be detected on output or error queues.
                    self.queues['input'].task_done()

        return _jobfunction

    def start(self):
        """Create threads and begin processing inputs"""

        for item in self.inputs:
            self.queues['input'].put(ThreadManager.QueueItem(priority=100,
                                     item=item))

        for i in range(self.max_threads):
            thread = threading.Thread(
                target=self.get_jobfunction(self.function))
            thread.start()
            self.threads.append(thread)

        return self

    def join(self):
        """Wait for all inputs to be processed"""
        self.queues['input'].join()
        if not self.queues['error'].empty():
            self.errors = []
            while not self.queues['error'].empty():
                try:
                    self.errors.append(self.queues['error'].get())
                except Queue.Empty:
                    pass
            jfe = ThreadManager.JobFailureException()
            jfe.errors = self.errors
            raise jfe

    def kill(self, blocking=False):
        """Stop running threads as soon as possible and don't process any
        more inputs"""
        for thread in self.threads:
            self.queues['input'].put(ThreadManager.QueueItem(priority=0,
                                     item=ThreadManager.ThreadStopSignal()))
        if blocking:
            while not all([t.is_alive() is False for t in self.threads]):
                time.sleep(0.01)
