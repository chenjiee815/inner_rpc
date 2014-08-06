#!/usr/bin/env python
# encoding=utf-8


"""
    inner_rpc.utils
    -----------------

    该模块主要包括一些零碎的方法或者类。

"""

import os
import os.path as osp
import logging
import struct
import cPickle

from gevent import Timeout, socket, sleep

from ir_exceptions import SocketTimeout, SocketError, DataError


class Picker(object):

    """简单的序列化与反序列化类."""

    def load(self, data):
        """反序列化."""
        return cPickle.loads(data)

    def dump(self, data):
        """序列化."""
        return cPickle.dumps(data, cPickle.HIGHEST_PROTOCOL)


class ServerSockets(object):

    """服务端的socket工厂.

    如果指定了 `unix_path` 则会创建 unix domain socket
    否则根据 `ip` 和 `port` 来创建 tcp socket

    :param ip: 服务端的IP
    :param port: 服务端端口号
    :param unix_path: 服务端的unix domain socket地址
    :param backlog: 允许同时连接到服务端的客户端数

    """

    def __init__(self, unix_path=None, ip='127.0.0.1', port=9000, backlog=1024):
        super(ServerSockets, self).__init__()
        self.__backlog = backlog
        if unix_path:
            if osp.exists(unix_path):
                os.unlink(unix_path)

            self.__address = unix_path
            self.__socket_family = socket.AF_UNIX
        else:
            self.__address = (ip, port)
            self.__socket_family = socket.AF_INET

    def __init_socket(self):
        try:
            s = socket.socket(self.__socket_family, socket.SOCK_STREAM)
            s.bind(self.__address)
            s.listen(self.__backlog)
            return s
        except socket.error:
            log = 'server {0}: create socket failed'.format(self.__address)
            raise SocketError(log)

    def get(self):
        """尝试获取socket，如果没有则立即抛出 `SocketError`"""
        return self.__init_socket()


class ClientSockets(object):

    """客户端的socket工厂.

    如果指定了 `unix_path` 则会创建 unix domain socket
    否则根据 `ip` 和 `port` 来创建 tcp socket

    :param ip: 服务端的IP
    :param port: 服务端端口号
    :param unix_path: 服务端的unix domain socket地址

    """

    def __init__(self, unix_path=None, ip='127.0.0.1', port=9000):
        super(ClientSockets, self).__init__()
        if unix_path:
            self.__address = unix_path
            self.__socket_family = socket.AF_UNIX
        else:
            self.__address = (ip, port)
            self.__socket_family = socket.AF_INET

    def __init_socket(self):
        try:
            s = socket.socket(self.__socket_family, socket.SOCK_STREAM)
            s.connect(self.__address)
            return s
        except socket.error:
            log = 'server {0}: connection failed'.format(self.__address)
            raise SocketError(log)

    def get(self):
        """尝试获取连接到服务端的socket，如果没有则立即抛出 `SocketError`"""
        return self.__init_socket()

    def bget(self, interval=0.3):
        """尝试获取连接到服务端的socket，直到成功获取."""
        while 1:
            try:
                return self.__init_socket()
            except SocketError:
                sleep(interval)


class Message(object):

    """RPC消息处理类.

    实现了RPC消息的协议的接收和发送

    当传入需要发送的数据，该类会自动组装成PubSub的内部消息结构然后发送

    接收端会自动解包接收到的PubSub内部消息结构，然后返回原始的数据

    :param socket_obj: socket对象，消息的接收和发送通过该对象实现
    :param recv_timeout: socket对象的接收超时，单位秒
    :param send_timeout: socket对象的发送超时，单位秒

    """

    def __init__(self, socket_obj, recv_timeout=None, send_timeout=None):
        super(Message, self).__init__()
        self.logger = logging.getLogger(__name__ + '.message')
        self.__socket = socket_obj
        self.__recv_timeout = recv_timeout
        self.__send_timeout = send_timeout
        self.__picker = Picker()
        # 内部消息头部格式
        self.__msglen_format = 'I'
        # 内部消息头部长度
        self.__msglen_format_len = struct.calcsize(self.__msglen_format)

    def get_socket(self):
        """获取socket对象."""
        return self.__socket

    def close_socket(self):
        """关闭socket对象."""
        self.__socket.close()
        return True

    def __recv_by_len(self, recv_len):
        """通过socket对象获取指定长度的数据."""
        def _socket_recv():
            rlen = recv_len
            while 1:
                msg = self.__socket.recv(rlen)
                msg_len = len(msg)
                if msg_len == 0:
                    break

                rlen -= msg_len
                yield msg

                if rlen <= 0:
                    break

        return ''.join(_socket_recv())

    def __recv_msg_head(self):
        # 消息头数据
        msglen_data = self.__recv_by_len(self.__msglen_format_len)
        if len(msglen_data) != self.__msglen_format_len:
            raise SocketError('recv msg head error')

        # 从消息头中解析出消息体的长度
        msg_body_len = struct.unpack(self.__msglen_format, msglen_data)[0]
        return msg_body_len

    def __recv(self):
        """内部的接收数据方法.

        该方法会将接收的数据根据内部消息协议解析出其中的消息体，

        然后再反序列化成原始的数据

        """
        msg_body_len = self.__recv_msg_head()
        # 消息体数据
        msg_data = self.__recv_by_len(msg_body_len)
        if len(msg_data) != msg_body_len:
            raise SocketError('recv msg body error')

        return self.__picker.load(msg_data)

    def __make_msg_data(self, data):
        mbody_data = self.__picker.dump(data)
        mbody_len = len(mbody_data)
        mhead_data = struct.pack(self.__msglen_format, mbody_len)
        msg_data = mhead_data + mbody_data
        return msg_data

    def __send(self, data):
        """内部的发送数据方法.

        该方法会将传入的数据先序列化， 然后组装成内部的数据结构再发送

        """
        msg_data = self.__make_msg_data(data)
        self.__socket.sendall(msg_data)
        return True

    def recv(self, _raise=True):
        """获取数据.

        :param _raise: 如果获取到的数据是一个Python异常，是否抛出该异常
        :param timeout: 是否指定该接口的超时时间

        """
        msg = None
        try:
            with Timeout(self.__recv_timeout, exception=SocketTimeout('recv_timeout')):
                msg = self.__recv()
        except socket.error as e:
            log = 'recv socket error'
            self.logger.exception(log)
            raise SocketError(e)
        except (struct.error, cPickle.PickleError, ValueError, TypeError) as e:
            log = 'recv data error'
            self.logger.exception(log)
            raise DataError(e)

        if _raise and isinstance(msg, BaseException):
            raise msg
        else:
            return msg

    def send(self, data):
        """发送数据.

        :param timeout: 是否指定该接口的超时时间

        """
        try:
            with Timeout(self.__send_timeout, exception=SocketTimeout('send timeout')):
                self.__send(data)
        except socket.error as e:
            log = 'send socket error'
            self.logger.exception(log)
            raise SocketError(e)
        except (struct.error, cPickle.PickleError, ValueError, TypeError) as e:
            log = 'send data error'
            self.logger.exception(log)
            raise DataError(e)
        else:
            return True


class RPCFuncs(object):

    """注册的PRC方法名与方法的映射类."""

    def __init__(self):
        super(RPCFuncs, self).__init__()
        self.__funcs = {}
        self.logger = logging.getLogger(__name__ + '.rpcfuncs')

    def register(self, func, func_name=None):
        """注册一个RPC方法，必须有对应的方法名."""
        if not callable(func):
            log = '`func` is not callable'
            self.logger.error(log)
            raise ValueError(log)

        if func_name:
            fname = func_name
        else:
            fname = getattr(func, 'func_name', None)

        if not fname:
            log = '`func`: func_name not found'
            self.logger.error(log)
            raise ValueError(log)

        self.__funcs[fname] = func
        return True

    def get(self, func_name):
        """根据方法名获取对应的RPC方法."""
        return self.__funcs.get(func_name, None)
