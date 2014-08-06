#!/usr/bin/env python
# encoding=utf-8

from utils import ClientSockets, Message

class Client(object):

    """
    RPC客户端

    :param ip: 服务端的IP
    :param port: 服务端端口号
    :param unix_path: 服务端的unix domain socket地址
    :param conn_block: 客户端是是否等待可用的服务端连接
    :param recv_timeout: 接收超时
    :param send_timeout: 发送超时

    """

    def __init__(self, unix_path=None, ip='127.0.0.1', port=10000,
                 conn_block=True, recv_timeout=None, send_timeout=None):
        super(Client, self).__init__()
        self.__sockets = ClientSockets(unix_path, ip, port)
        self.__conn_block = conn_block
        self.__recv_timeout = recv_timeout
        self.__send_timeout = send_timeout

    def __init_socket(self):
        if self.__conn_block:
            return self.__sockets.bget()
        else:
            return self.__sockets.get()

    def __getattr__(self, name):
        return lambda *args, **kwargs: self.__method(
            name, args, kwargs)

    def __method(self, name, args, kwargs):
        rpc_socket = self.__init_socket()
        message = Message(rpc_socket, self.__recv_timeout, self.__send_timeout)
        smsg = {'func': name,
                'args': args,
                'kwargs': kwargs}
        message.send(smsg)
        try:
            rmsg = message.recv()
        finally:
            message.close_socket()

        if isinstance(rmsg, BaseException):
            raise rmsg
        else:
            return rmsg    
