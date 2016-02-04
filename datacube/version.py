# coding=utf-8
"""
Version information from setuptools installed package.

May fail or return the wrong version when running in an
un-installed state in develop.
"""
from __future__ import absolute_import
import pkg_resources

#: pylint: disable=not-callable
VERSION = pkg_resources.require('datacube')[0].version
