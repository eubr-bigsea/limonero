# -*- coding: utf-8 -*-
import datetime
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
class DataSourceFormat:
    XML_FILE = 'XML_FILE'
    NETCDF4 = 'NETCDF4'
    HDF5 = 'HDF5'
    SHAPEFILE = 'SHAPEFILE'
    TEXT = 'TEXT'
    CUSTOM = 'CUSTOM'
    JSON = 'JSON'
    CSV = 'CSV'
    PICKLE = 'PICKLE'

    @staticmethod
    def values():
        return [n for n in DataSourceFormat.__dict__.keys()
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class StorageType:
    HDFS = 'HDFS'
    OPHIDIA = 'OPHIDIA'
    ELASTIC_SEARCH = 'ELASTIC_SEARCH'
    MONGODB = 'MONGODB'
    POSTGIS = 'POSTGIS'
    HBASE = 'HBASE'
    CASSANDRA = 'CASSANDRA'

    @staticmethod
    def values():
        return [n for n in StorageType.__dict__.keys()
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class DataType:
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

    @staticmethod
    def values():
        return [n for n in DataType.__dict__.keys()
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class PrivacyRiskType:
    IDENTIFICATION = 'IDENTIFICATION'

    @staticmethod
    def values():
        return [n for n in PrivacyRiskType.__dict__.keys()
                if n[0] != '_' and n != 'values']


class Attribute(db.Model):
    """ Data source attribute. """
    __tablename__ = 'attribute'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(500))
    type = Column(Enum(*DataType.values(),
                       name='DataTypeEnumType'), nullable=False)
    size = Column(Integer)
    precision = Column(Integer)
    nullable = Column(Boolean, nullable=False)
    enumeration = Column(Boolean, nullable=False)
    missing_representation = Column(String(200))
    feature = Column(Boolean,
                     default=True, nullable=False)
    label = Column(Boolean,
                   default=True, nullable=False)
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


class AttributePrivacy(db.Model):
    """ Privacy configuration for an attribute. """
    __tablename__ = 'attribute_privacy'

    # Fields
    id = Column(Integer, primary_key=True)
    privacy_attribute_type = Column(String(100), nullable=False)
    category_technique = Column(String(100), nullable=False)
    anonymization_technique = Column(String(100), nullable=False)
    hierarchical_structure_type = Column(String(100), nullable=False)
    hierarchy = Column(String(100), nullable=False)
    category_model = Column(String(100), nullable=False)
    privacy_model = Column(String(100), nullable=False)
    privacy_model_technique = Column(String(100), nullable=False)
    privacy_model_parameters = Column(Text, nullable=False)

    # Associations
    attribute_id = Column(Integer,
                          ForeignKey("attribute.id"), nullable=False)
    attribute = relationship("Attribute", foreign_keys=[attribute_id],
                             backref=backref(
                                 "attribute_privacy",
                                 cascade="all, delete-orphan"))

    def __unicode__(self):
        return self.privacy_attribute_type

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class DataSource(db.Model):
    """ Data source in Lemonade system (anything that stores data. """
    __tablename__ = 'data_source'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(500))
    enabled = Column(Boolean,
                     default=True, nullable=False)
    read_only = Column(Boolean,
                       default=True, nullable=False)
    privacy_aware = Column(Boolean,
                           default=False, nullable=False)
    url = Column(String(200), nullable=False)
    created = Column(DateTime,
                     default=func.now(), nullable=False)
    format = Column(Enum(*DataSourceFormat.values(),
                         name='DataSourceFormatEnumType'), nullable=False)
    provenience = Column(Text)
    estimated_rows = Column(Integer)
    estimated_size_in_mega_bytes = Column(Numeric(10, 2))
    expiration = Column(String(200))
    user_id = Column(Integer)
    user_login = Column(String(50))
    user_name = Column(String(200))
    tags = Column(String(100))
    temporary = Column(Boolean,
                       default=False, nullable=False)
    workflow_id = Column(Integer)
    task_id = Column(String(200))
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


class PrivacyRisk(db.Model):
    """ Privacy information associated to the data source """
    __tablename__ = 'privacy_risk'

    # Fields
    id = Column(Integer, primary_key=True)
    type = Column(Enum(*PrivacyRiskType.values(),
                       name='PrivacyRiskTypeEnumType'), nullable=False)
    probability = Column(Float)
    impact = Column(Float)
    value = Column(Float, nullable=False)
    detail = Column(Text, nullable=False)

    # Associations
    data_source_id = Column(Integer,
                            ForeignKey("data_source.id"), nullable=False)
    data_source = relationship("DataSource", foreign_keys=[data_source_id],
                               backref=backref(
                                   "risks",
                                   cascade="all, delete-orphan"))

    def __unicode__(self):
        return self.type

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class Storage(db.Model):
    """ Type of storage used by data sources """
    __tablename__ = 'storage'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    type = Column(Enum(*StorageType.values(),
                       name='StorageTypeEnumType'), nullable=False)
    url = Column(String(1000), nullable=False)

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)

