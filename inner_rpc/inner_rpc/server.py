#!/usr/bin/env python
# encoding=utf-8

import logging

from gevent.server import StreamServer

from utils import ServerSockets, Message, RPCFuncs


class Server(object):

    """RPC服务端.

    :param ip: IP地址
    :param port: 端口号
    :param unix_path: 服务端的unix domain socket地址
    :param backlog: 同时可连接的客户端数
    :param recv_timeout: 接收超时
    :param send_timeout: 发送超时

    """

    def __init__(self, unix_path=None, ip='127.0.0.1', port=10000, backlog=1024,
                 recv_timeout=3, send_timeout=3, debug=False):
        super(Server, self).__init__()
        self.logger = logging.getLogger(__name__)
        self.__sockets = ServerSockets(unix_path, ip, port, backlog=backlog)
        self.__recv_timeout = recv_timeout
        self.__send_timeout = send_timeout
        self.__debug = debug
        self.__rpcfuncs = RPCFuncs()
        server_socket = self.__sockets.get()
        handle_socket = lambda sobj, addr: self.__handle_socket(sobj)
        self.__server = StreamServer(server_socket, handle_socket)

    def register(self, func, func_name=None):
        """注册可调用的RPC方法."""
        return self.__rpcfuncs.register(func, func_name=func_name)

    def __load_msg(self, msg):
        if not hasattr(msg, 'get'):
            log = 'src_msg: has no `get`'
            self.logger.error(log)
            raise ValueError(log)

        func_name = msg.get('func', None)
        if func_name is None:
            log = 'src_msg: `func` not found'
            self.logger.error(log)
            return None, None, None

        func = self.__rpcfuncs.get(func_name)
        if func is None:
            log = 'func_name {0}: has not been registerd'.format(func_name)
            self.logger.error(log)
            return None, None, None

        args = msg.get('args', ())
        kwargs = msg.get('kwargs', {})
        return func, args, kwargs

    def __work(self, func, args, kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if self.__debug:
                func_name = getattr(func, 'func_name', 'inner_rpc_default')
                log = 'func {0}: error'.format(func_name)
                self.logger.exception(log)
            return e

    def __handle_socket(self, sobj):
        message = Message(sobj, self.__recv_timeout, self.__send_timeout)
        src_msg = message.recv(_raise=False)
        func, args, kwargs = self.__load_msg(src_msg)
        if func:
            smsg = self.__work(func, args, kwargs)
        else:
            smsg = ValueError('not such function')

        try:
            message.send(smsg)
        except Exception:
            self.logger.exception('send result error')
        finally:
            message.close_socket()

    def start(self):
        self.__server.serve_forever()

    def stop(self):
        self.__server.stop()


def main():
    logging.basicConfig(level=logging.DEBUG)

    def hello(name):
        return 'hello {0}'.format(name)

    s = Server(unix_path='/tmp/test.sock')
    s.register(hello)
    s.start()


if __name__ == '__main__':
    main()
