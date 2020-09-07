# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

# Separated test code with Python 3.6 syntax.

import typing
import decimal
from streamsx.spl.types import int64

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
    si64: typing.Set[int]
    oi64: typing.Optional[int] = None
    of64: typing.Optional[float] = None
    od128: typing.Optional[decimal.Decimal] = None
    oc64: typing.Optional[complex] = None
    omi64li64: typing.Optional[typing.Mapping[int,typing.List[int]]] = None


class SpottedSchema(typing.NamedTuple):
    start_time: float
    end_time: float
    confidence: float

class NamedTupleSetOfListofTupleSchema(typing.NamedTuple):
    slt: typing.Set[typing.List[SpottedSchema]]

class NamedTupleMapWithTupleSchema(typing.NamedTuple):
    keywords_spotted: typing.Mapping[str,SpottedSchema]

class NamedTupleMapWithListTupleSchema(typing.NamedTuple):
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

#tuple<int64 x_coord, int64 y_coord>
class Point2DSchema(typing.NamedTuple):
    x_coord: int
    y_coord: int
    
#tuple<int64 x_coord, int64 y_coord, int64 z_coord>
class Point3DSchema(typing.NamedTuple):
    x_coord: int
    y_coord: int
    z_coord: int

#tuple<tuple<int64 x_coord, int64 y_coord> center, int64 radius>
class CircleSchema(typing.NamedTuple):
    center: Point2DSchema
    radius: float

#tuple<tuple<int64 x_coord, int64 y_coord, int64 z_coord> center, int64 radius , int64 radius2>
class DonutSchema(typing.NamedTuple):
    center: Point3DSchema
    radius: int
    radius2: int
    rings: bool
    #rings: typing.List[CircleSchema]

#tuple<tuple<tuple<int64 x_coord, int64 y_coord> center, radius int64> circle, 
#      tuple<tuple<int64 x_coord, int64 y_coord, int64 z_coord> center, int64 radius , int64 radius2> torus>
class TripleNestedTupleAmbiguousAttrName(typing.NamedTuple):
    circle: CircleSchema    # contains 'center' as tuple attribute
    torus: DonutSchema      # contains also 'center' as a different tuple type attribute, contains 'rings' attribute
    rings: typing.List[CircleSchema]  # rings with nested (anonymous C++ type)
    
    

