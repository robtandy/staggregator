#!/usr/bin/env python

from distutils.core import setup
import staggregator

V = staggregator.__version__

setup(name='staggregator',
      version=V,
      author='Rob Tandy',
      author_email='rob.tandy@gmail.com',
      url='https://github.com/robtandy/staggregator',
      long_description="""
      A stats aggregator for graphite, in the spirit of statsd.""",
      packages=['staggregator'],
)
