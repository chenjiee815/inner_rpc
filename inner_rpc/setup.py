#!/usr/bin/env python
# encoding=utf-8

from setuptools import setup

setup(name='inner_rpc',
      version="0.1.0",
      description='simple rpc',
      url="http://wwww.vulnhunt.com/",
      author="vulnhunt",
      author_email="list_xingyun@vulnhunt.com",
      license="commercial",
      test_suite = 'nose.collector',
      packages=["inner_rpc"],
      zip_safe=False,
      # install_requires=[
      #     "gevent",
      #  ],
     )
