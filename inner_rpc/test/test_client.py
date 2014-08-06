#!/usr/bin/env python
# encoding=utf-8

import os.path as osp
import unittest
import subprocess
import time

from inner_rpc import Client

SERVER = None

UNIX_PATH = "/tmp/test_inner_rpc.socket"

def setUp():
    global SERVER
    server_script = osp.join(
        osp.dirname(__file__), 'scripts', 'server.py')
    SERVER = subprocess.Popen(['python', server_script, UNIX_PATH])
    time.sleep(1)


def tearDown():
    if SERVER:
        SERVER.terminate()
        time.sleep(1)


class ClientTestCase(unittest.TestCase):

    def setUp(self):
        return True

    def tearDown(self):
        return True

    def test_hello(self):
        c = Client(unix_path=UNIX_PATH, conn_block=False, recv_timeout=1, send_timeout=1)
        ret = c.hello("test")
        self.assertEqual("hello test", ret)

    def test_error(self):
        c = Client(unix_path=UNIX_PATH, conn_block=False, recv_timeout=1, send_timeout=1)
        with self.assertRaises(SystemError):
            c.error()


if __name__ == '__main__':
    unittest.main()
