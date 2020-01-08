# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020

"""
Definition of common types used for type hints.
"""

from typing import Any, Type, Union, NamedTuple

Schema = Union[
    'streamsx.topology.schema.StreamSchema',
    'streamsx.topology.schema.CommonSchema',
    str, NamedTuple]
