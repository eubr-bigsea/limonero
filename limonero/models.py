# -*- coding: utf-8 -*-
import datetime
import enum
import json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float, \
    Enum, DateTime, Numeric, Text, Unicode, UnicodeText
from sqlalchemy import event
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref
from sqlalchemy_i18n import make_translatable, translation_base, Translatable

make_translatable(options={'locales': ['pt', 'en', 'es'],
                           'auto_create_locales': True,
                           'fallback_locale': 'en'})

db = SQLAlchemy()


# noinspection PyClassHasNoInit
class DataSourceFormat(enum.Enum):
    XML_FILE = 'XML_FILE'
    NETCDF4 = 'NETCDF4'
    HDF5 = 'HDF5'
    SHAPEFILE = 'SHAPEFILE'
    TEXT = 'TEXT'
    CUSTOM = 'CUSTOM'
    JSON = 'JSON'
    CSV = 'CSV'
    PICKLE = 'PICKLE'


# noinspection PyClassHasNoInit
class StorageType(enum.Enum):
    HDFS = 'HDFS'
    OPHIDIA = 'OPHIDIA'
    ELASTIC_SEARCH = 'ELASTIC_SEARCH'
    MONGODB = 'MONGODB'
    POSTGIS = 'POSTGIS'
    HBASE = 'HBASE'
    CASSANDRA = 'CASSANDRA'


# noinspection PyClassHasNoInit
class DataType(enum.Enum):
    FLOAT = 'FLOAT'
    LAT_LONG = 'LAT_LONG'
    TIME = 'TIME'
    DOUBLE = 'DOUBLE'
    DECIMAL = 'DECIMAL'
    ENUM = 'ENUM'
    CHARACTER = 'CHARACTER'
    LONG = 'LONG'
    DATETIME = 'DATETIME'
    VECTOR = 'VECTOR'
    TEXT = 'TEXT'
    DATE = 'DATE'
    INTEGER = 'INTEGER'
    TIMESTAMP = 'TIMESTAMP'


class Attribute(db.Model):
    """ Data source attribute. """
    __tablename__ = 'attribute'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(500))
    type = Column(Enum(*DataType.__members__.keys(),
                       name='DataTypeEnumType'), nullable=False)
    size = Column(Integer)
    precision = Column(Integer)
    nullable = Column(Boolean, nullable=False)
    enumeration = Column(Boolean, nullable=False)
    missing_representation = Column(String(200))
    feature = Column(Boolean, nullable=False, default=True)
    label = Column(Boolean, nullable=False, default=True)
    distinct_values = Column(Integer)
    mean_value = Column(Float)
    median_value = Column(String(200))
    max_value = Column(String(200))
    min_value = Column(String(200))
    std_deviation = Column(Float)
    missing_total = Column(String(200))
    deciles = Column(Text)

    # Associations
    data_source_id = Column(Integer,
                            ForeignKey("data_source.id"), nullable=False)
    data_source = relationship("DataSource", foreign_keys=[data_source_id],
                               backref=backref(
                                   "attributes",
                                   cascade="all, delete-orphan"))

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class DataSource(db.Model):
    """ Data source in Lemonade system (anything that stores data. """
    __tablename__ = 'data_source'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(500))
    enabled = Column(Boolean, nullable=False, default=True)
    read_only = Column(Boolean, nullable=False, default=True)
    url = Column(String(200), nullable=False)
    created = Column(DateTime, nullable=False, default=func.now())
    format = Column(Enum(*DataSourceFormat.__members__.keys(),
                         name='DataSourceFormatEnumType'), nullable=False)
    provenience = Column(Text)
    estimated_rows = Column(Integer)
    estimated_size_in_mega_bytes = Column(Numeric(10, 2))
    expiration = Column(String(200))
    user_id = Column(Integer)
    user_login = Column(String(50))
    user_name = Column(String(200))
    tags = Column(String(100))
    temporary = Column(Boolean, nullable=False, default=False)
    workflow_id = Column(Integer)
    task_id = Column(Integer)
    __mapper_args__ = {
        'order_by': 'name'
    }

    # Associations
    storage_id = Column(Integer,
                        ForeignKey("storage.id"), nullable=False)
    storage = relationship("Storage", foreign_keys=[storage_id])

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class Storage(db.Model):
    """ Type of storage used by data sources """
    __tablename__ = 'storage'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    type = Column(Enum(*StorageType.__members__.keys(),
                       name='StorageTypeEnumType'), nullable=False)
    url = Column(String(1000), nullable=False)

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)

