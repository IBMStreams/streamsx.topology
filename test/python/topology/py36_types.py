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

class NamedTupleNumbersSchema2(typing.NamedTuple):
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

#tuple<float64 start_time, float64 end_time, float64 confidence>
class SpottedSchema(typing.NamedTuple):
    start_time: float
    end_time: float
    confidence: float

class NamedTupleSetOfListofTupleSchema(typing.NamedTuple):
    slt: typing.Set[typing.List[SpottedSchema]]

#tuple<map<rstring, tuple<float64 start_time, float64 end_time, float64 confidence>> keywords_spotted>
class NamedTupleMapWithTupleSchema(typing.NamedTuple):
    keywords_spotted: typing.Mapping[str,SpottedSchema]

class NamedTupleMapWithListTupleSchema(typing.NamedTuple):
    keywords_spotted: typing.Mapping[str,typing.List[SpottedSchema]]

class NamedTupleListOfTupleSchema(typing.NamedTuple):
    spotted: typing.List[SpottedSchema]

#tuple<rstring str, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>
class NamedTupleNestedTupleSchema(typing.NamedTuple):
    key: str
    spotted: SpottedSchema
    
#tuple<int64 i64, list<tuple<rstring str, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>> spottedList>
class NamedTupleListOfNestedTupleSchema(typing.NamedTuple):
    i64: int
    spottedList: typing.List[NamedTupleNestedTupleSchema]

#tuple<rstring s1, tuple<int64 i64, list<tuple<rstring key, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>> spottedList> tupleWList>
class NamedTupleNestedList2Schema(typing.NamedTuple):
    s1: str
    tupleWList: NamedTupleListOfNestedTupleSchema

#tuple<rstring s2, tuple<rstring s1, tuple<int64 i64, list<tuple<rstring key, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>> spottedList> tupleWList> tupleWList2>
class NamedTupleNestedList3Schema(typing.NamedTuple):
    s2: str
    tupleWList2: NamedTupleNestedList2Schema
    
#tuple<int64 i64, map<rstring, tuple<rstring str, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>> spotted>
class NamedTupleMapOfNestedTupleSchema(typing.NamedTuple):
    i64: int
    spottedMap: typing.Mapping[str,NamedTupleNestedTupleSchema]

#tuple<rstring s1, tuple<int64 i64, map<rstring, tuple<rstring key, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>> spottedMap> tupleWMap>
class NamedTupleNestedMap2Schema(typing.NamedTuple):
    s1: str
    tupleWMap: NamedTupleMapOfNestedTupleSchema

#tuple<rstring s2, tuple<rstring s1, tuple<int64 i64, map<rstring,tuple<rstring key, tuple<float64 start_time, float64 end_time, float64 confidence> spotted>> spottedMap> tupleWMap> tupleWMap2>
class NamedTupleNestedMap3Schema(typing.NamedTuple):
    s2: str
    tupleWMap2: NamedTupleNestedMap2Schema


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

#tuple<float64 radius, boolean has_rings>
class CircleRadiusSchema(typing.NamedTuple):
    radius: float
    has_rings: bool

#tuple<tuple<int64 x_coord, int64 y_coord, int64 z_coord> center, int64 radius , int64 radius2>
class DonutSchema(typing.NamedTuple):
    center: Point3DSchema
    radius: int
    radius2: int
    rings: typing.List[CircleRadiusSchema]

#tuple<tuple<tuple<int64 x_coord, int64 y_coord> center, radius int64> circle, 
#      tuple<tuple<int64 x_coord, int64 y_coord, int64 z_coord> center, int64 radius , int64 radius2> torus>
class TripleNestedTupleAmbiguousAttrName(typing.NamedTuple):
    circle: CircleSchema    # contains 'center' as tuple attribute
    torus: DonutSchema      # contains also 'center' as a different tuple type attribute, contains 'rings' attribute
    rings: typing.List[CircleSchema]  # rings with nested (anonymous C++ type)

#tuple<int64 int1, map<string, tuple<int64 x_coord, int64 y_coord>> map1>
class TupleWithMapToTupleAttr1(typing.NamedTuple):
    int1: int
    map1: typing.Mapping[str,Point2DSchema]

#tuple<int64 int2, map<string, tuple<int64 int1, map<rstring, tuple<int64 x_coord, int64 y_coord>> map1>> map2>
# This schema contains map attributes at different nesting levels with different attribute names and different Value types
class TupleWithMapToTupleWithMap(typing.NamedTuple):
    int2: int
    map2: typing.Mapping[str,TupleWithMapToTupleAttr1]

#tuple<int64 int1, map<string, tuple<int64 int1, map<rstring, tuple<int64 x_coord, int64 y_coord>> map1>> map1>
# This schema contains map attributes at different nesting levels with equal map attribute name (map1), but different Value types
class TupleWithMapToTupleWithMapAmbigousMapNames(typing.NamedTuple):
    int1: int
    map1: typing.Mapping[str,TupleWithMapToTupleAttr1]

#tuple<int64 int1, map<string, tuple<int64 x_coord, int64 y_coord, int64 z_coord>> map1>
#class TupleWithMapToTupleAttr2(typing.NamedTuple):
#    int1: int
#    map1: typing.Mapping[str,Point3DSchema]

