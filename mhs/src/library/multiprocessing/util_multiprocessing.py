"""
Author: Diego Pinheiro
github: https://github.com/diegompin

"""
import multiprocessing
# import multiprocessing.pool
#
# class NoDaemonProcess(multiprocessing.Process):
#     @property
#     def daemon(self):
#         return False
#
#     @daemon.setter
#     def daemon(self, value):
#         pass
#
#
# class NoDaemonContext(type(multiprocessing.get_context())):
#     Process = NoDaemonProcess
#
#
# # We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# # because the latter is only a wrapper function, not a proper class.
# class MyPool(multiprocessing.Pool):
#     def __init__(self, *args, **kwargs):
#         kwargs['context'] = NoDaemonContext()
#         super(MyPool, self).__init__(*args, **kwargs)
#


# class NonDaemonPool(multiprocessing.pool.Pool):
#     def Process(self, *args, **kwds):
#         proc = super(NonDaemonPool, self).Process(*args, **kwds)
#
#         class NonDaemonProcess(proc.__class__):
#             """Monkey-patch process to ensure it is never daemonized"""
#
#             @property
#             def daemon(self):
#                 return False
#
#             @daemon.setter
#             def daemon(self, val):
#                 pass
#
#         proc.__class__ = NonDaemonProcess
#
#         return proc



#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import multiprocessing
# We must import this explicitly, it is not imported by the top-level
# multiprocessing module.
import multiprocessing.pool
import time

from random import randint


class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess
