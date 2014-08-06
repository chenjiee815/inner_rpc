#!/usr/bin/env python
# encoding=utf-8

"""
    inner_rpc.ir_exceptions
    -----------------

    该模块主要包括公共的异常定义
"""

class BaseError(Exception):
    pass

class SocketError(BaseError):
    pass

class SocketTimeout(SocketError):
    pass

class DataError(BaseError):
    pass
