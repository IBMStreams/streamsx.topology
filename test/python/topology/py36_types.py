# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

# Separated test code with Python 3.6 syntax.

import typing
import decimal

class NTS(typing.NamedTuple):
    x: int
    msg: str


class NamedTupleBytesSchema(typing.NamedTuple):
    idx: str
    msg: bytes
    flag: bool
    oidx: typing.Optional[str] = None
    omsg: typing.Optional[bytes] = None
    oflag: typing.Optional[bool] = None


class NamedTupleNumbersSchema(typing.NamedTuple):
    i64: int
    f64: float
    d128: decimal.Decimal
    c64: complex
    oi64: typing.Optional[int] = None
    of64: typing.Optional[float] = None
    od128: typing.Optional[decimal.Decimal] = None
    oc64: typing.Optional[complex] = None
    omi64li64: typing.Optional[typing.Mapping[int,typing.List[int]]] = None

class SpottedSchema(typing.NamedTuple):
    start_time: float
    end_time: float
    confidence: float

class NamedTupleListWithCustomInMapTupleSchema(typing.NamedTuple):
    keywords_spotted: typing.Mapping[str,typing.List[SpottedSchema]]

class NamedTupleListOfTupleSchema(typing.NamedTuple):
    spotted: typing.List[SpottedSchema]

class NamedTupleNestedTupleSchema(typing.NamedTuple):
    key: str
    spotted: SpottedSchema

class TestSchema(typing.NamedTuple):
    flag: bool
    i64: int

class ContactsSchema(typing.NamedTuple):
    mail: str
    phone: str
    nested_tuple: TestSchema

class AddressSchema(typing.NamedTuple):
    street: str
    city: str
    contacts: ContactsSchema

class PersonSchema(typing.NamedTuple):
    name: str
    age: int
    address: AddressSchema


