#!/usr/bin/env python
# encoding=utf-8

import os
import os.path as osp
import random
import unittest
import socket

from inner_rpc import utils, ir_exceptions


class ClientTestCase(unittest.TestCase):

    def setUp(self):
        self.__server_socket = None
        self.__client_socket = None
        self.__unix_path = '/tmp/test_inner_rpc_utils.sock'
        return True

    def tearDown(self):
        if self.__server_socket:
            self.__server_socket.close()

        if self.__client_socket:
            self.__client_socket.close()

        if osp.isfile(self.__unix_path):
            os.unlink(self.__unix_path)

        return True

    def __get_random_string(self, length):
        get_rand_chr = lambda: chr(random.randint(0, 0xFF))
        rand_str = (get_rand_chr() for _ in xrange(length))
        return ''.join(rand_str)

    def test_picker(self):
        picker = utils.Picker()
        test_data = self.__get_random_string(1024)
        dump_data = picker.dump(test_data)
        load_data = picker.load(dump_data)
        self.assertEqual(test_data, load_data)

    def test_serversockets_ipport(self):
        ss = utils.ServerSockets(ip='127.0.0.1', port=9000)
        self.__server_socket = ss.get()
        self.assertEqual(self.__server_socket.family, socket.AF_INET)
        self.assertEqual(self.__server_socket.type, socket.SOCK_STREAM)

    def test_serversockets_unixpath(self):
        ss = utils.ServerSockets(unix_path=self.__unix_path)
        self.__server_socket = ss.get()
        self.assertEqual(self.__server_socket.family, socket.AF_UNIX)
        self.assertEqual(self.__server_socket.type, socket.SOCK_STREAM)

    def test_clientsockets_ipport(self):
        cs = utils.ClientSockets(ip='127.0.0.1', port=9000)
        with self.assertRaises(ir_exceptions.SocketError):
            cs.get()

        ss = utils.ServerSockets(ip='127.0.0.1', port=9000)
        self.__server_socket = ss.get()
        self.__client_socket = cs.get()
        self.assertEqual(self.__client_socket.family, socket.AF_INET)
        self.assertEqual(self.__client_socket.type, socket.SOCK_STREAM)

    def test_clientsockets_unixpath(self):
        cs = utils.ClientSockets(unix_path=self.__unix_path)
        with self.assertRaises(ir_exceptions.SocketError):
            cs.get()

        ss = utils.ServerSockets(unix_path=self.__unix_path)
        self.__server_socket = ss.get()
        self.__client_socket = cs.get()
        self.assertEqual(self.__client_socket.family, socket.AF_UNIX)
        self.assertEqual(self.__client_socket.type, socket.SOCK_STREAM)

    def test_rpcfuncs(self):
        rf = utils.RPCFuncs()
        test_func = lambda: True
        rf.register(test_func, 'test_func_name')
        self.assertEqual(test_func, rf.get('test_func_name'))


if __name__ == '__main__':
    unittest.main()
