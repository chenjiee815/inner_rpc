#!/usr/bin/env python
# encoding=utf-8

import sys
from inner_rpc import Server as RPCServer


def hello(name):
    return "hello {0}".format(name)

def error():
    raise SystemError("error")

def main():
    unix_path = sys.argv[1]
    s = RPCServer(unix_path=unix_path)
    s.register(hello)
    s.register(error)
    s.start()

if __name__ == '__main__':
   main()
