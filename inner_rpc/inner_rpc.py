#!/usr/bin/env python
# encoding=utf-8

import os
import os.path as osp
import struct
import cPickle

from gevent.server import StreamServer
from gevent import socket, Timeout, sleep

def socket_recv(socket_obj, recv_len):
    def _socket_recv(recv_len):
        while 1:
            msg = socket_obj.recv(recv_len)
            recv_len -= len(msg)
            yield msg
            if recv_len <= 0:
                break

    return "".join(_socket_recv(recv_len))


class Server(object):
    def __init__(self, unix_socket_path, listen_num=5, ack_timeout=3):
        super(Server, self).__init__()
        self.unix_socket_path = unix_socket_path
        self.listen_num = listen_num
        self.ack_timeout = 3
        self.mlen_format = "I"
        self.mlen_format_len = struct.calcsize(self.mlen_format)
        self.server = None
        self.funcs = {}

    def get_rpc_socket(self):
        if osp.exists(self.unix_socket_path):
            os.unlink(self.unix_socket_path)
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(self.unix_socket_path)
        s.listen(self.listen_num)
        return s

    def register(self, func):
        if not callable(func):
            err = 'the arg is not callable'
            raise ValueError(err)
        else:
            self.funcs[func.func_name] = func
            return True

    def load_msg(self, msg):
        msg = cPickle.loads(msg)
        func_name = msg.get('func', None)
        if func_name is None:
            return None, None, None

        func = self.funcs.get(func_name, None)
        if func is None:
            return None, None, None

        args = msg.get('args', ())
        kwargs = msg.get('kwargs', {})
        return func, args, kwargs

    def _work(self, func, args, kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return e

    def handle_socket(self, sobj):
        """子类需要实现的方法, 用来接收和处理socket_obj发送来的消息."""
        try:
            rmsg_len = socket_recv(sobj, self.mlen_format_len)
        except socket.error:
            sobj.close()
            return

        rmsg_len = struct.unpack(self.mlen_format, rmsg_len)[0]
        try:
            rmsg = socket_recv(sobj, rmsg_len)
        except socket.error:
            sobj.close()
            return

        func, args, kwargs = self.load_msg(rmsg)
        if func:
            smsg = self._work(func, args, kwargs)
        else:
            smsg = ValueError('not such function')
        smsg = cPickle.dumps(smsg, 2)
        smsg = struct.pack(self.mlen_format, len(smsg)) + smsg

        try:
            with Timeout(self.ack_timeout):
                sobj.sendall(smsg)
        except (Timeout, socket.error):
            sobj.close()

    def start(self):
        self.server = StreamServer(
            self.get_rpc_socket(),
            lambda sobj, addr: self.handle_socket(sobj))
        self.server.serve_forever()

    def stop(self):
        if self.server:
            self.server.stop()

    def end(self):
        if self.server:
            self.server.kill()


class Client(object):
    def __init__(self, unix_socket_path, con_block=True):
        super(Client, self).__init__()
        self.unix_socket_path = unix_socket_path
        self.con_block = con_block
        self.mlen_format = "I"
        self.mlen_format_len = struct.calcsize(self.mlen_format)

    def __yield_socket(self):
        while 1:
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect(self.unix_socket_path)
            except socket.error as e:
                if self.con_block:
                    sleep(0.1)
                    continue
                else:
                    raise e
            else:
                yield s

    def __getattr__(self, name):
        return lambda *args, **kwargs: self._method(
            name, args, kwargs)

    def _method(self, name, args, kwargs):
        rpc_socket = self.__yield_socket().next()
        smsg = {'func': name,
                'args': args,
                'kwargs': kwargs}
        smsg = cPickle.dumps(smsg, 2)
        smsg = struct.pack(self.mlen_format, len(smsg)) + smsg
        rpc_socket.sendall(smsg)
        rmsg_len = socket_recv(rpc_socket, self.mlen_format_len)
        rmsg_len = struct.unpack(self.mlen_format, rmsg_len)[0]
        rmsg = socket_recv(rpc_socket, rmsg_len)
        rpc_socket.close()
        rmsg = cPickle.loads(rmsg)
        if isinstance(rmsg, BaseException):
            raise rmsg
        else:
            return rmsg
