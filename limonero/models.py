# -*- coding: utf-8 -*-
import datetime
import json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float, \
    Enum, DateTime, Numeric, Text, Unicode, UnicodeText
from sqlalchemy import event
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy_i18n import make_translatable, translation_base, Translatable

make_translatable(options={'locales': ['pt', 'en'],
                           'auto_create_locales': True,
                           'fallback_locale': 'en'})

db = SQLAlchemy()


# noinspection PyClassHasNoInit
class DataSourceFormat:
    CSV = 'CSV'
    CUSTOM = 'CUSTOM'
    GEO_JSON = 'GEO_JSON'
    JDBC = 'JDBC'
    IMAGE_FOLDER = 'IMAGE_FOLDER'
    DATA_FOLDER = 'DATA_FOLDER'
    HAR_IMAGE_FOLDER = 'HAR_IMAGE_FOLDER'
    HDF5 = 'HDF5'
    JSON = 'JSON'
    NETCDF4 = 'NETCDF4'
    PICKLE = 'PICKLE'
    PARQUET = 'PARQUET'
    SHAPEFILE = 'SHAPEFILE'
    TAR_IMAGE_FOLDER = 'TAR_IMAGE_FOLDER'
    TEXT = 'TEXT'
    VIDEO_FOLDER = 'VIDEO_FOLDER'
    XML_FILE = 'XML_FILE'
    UNKNOWN = 'UNKNOWN'

    @staticmethod
    def values():
        return [n for n in list(DataSourceFormat.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class ModelType:
    KERAS = 'KERAS'
    SPARK_ML_CLASSIFICATION = 'SPARK_ML_CLASSIFICATION'
    SPARK_ML_REGRESSION = 'SPARK_ML_REGRESSION'
    SPARK_MLLIB_CLASSIFICATION = 'SPARK_MLLIB_CLASSIFICATION'
    UNSPECIFIED = 'UNSPECIFIED'

    @staticmethod
    def values():
        return [n for n in list(ModelType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class StorageType:
    MONGODB = 'MONGODB'
    OPHIDIA = 'OPHIDIA'
    POSTGIS = 'POSTGIS'
    ELASTIC_SEARCH = 'ELASTIC_SEARCH'
    HBASE = 'HBASE'
    HDFS = 'HDFS'
    LOCAL = 'LOCAL'
    JDBC = 'JDBC'
    CASSANDRA = 'CASSANDRA'

    @staticmethod
    def values():
        return [n for n in list(StorageType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class DataType:
    BINARY = 'BINARY'
    CHARACTER = 'CHARACTER'
    DATE = 'DATE'
    DATETIME = 'DATETIME'
    DECIMAL = 'DECIMAL'
    DOUBLE = 'DOUBLE'
    ENUM = 'ENUM'
    FLOAT = 'FLOAT'
    INTEGER = 'INTEGER'
    LAT_LONG = 'LAT_LONG'
    LONG = 'LONG'
    TEXT = 'TEXT'
    TIME = 'TIME'
    TIMESTAMP = 'TIMESTAMP'
    VECTOR = 'VECTOR'

    @staticmethod
    def values():
        return [n for n in list(DataType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class PrivacyRiskType:
    IDENTIFICATION = 'IDENTIFICATION'

    @staticmethod
    def values():
        return [n for n in list(PrivacyRiskType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class PermissionType:
    READ = 'READ'
    WRITE = 'WRITE'
    MANAGE = 'MANAGE'

    @staticmethod
    def values():
        return [n for n in list(PermissionType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class AnonymizationTechnique:
    ENCRYPTION = 'ENCRYPTION'
    GENERALIZATION = 'GENERALIZATION'
    SUPPRESSION = 'SUPPRESSION'
    MASK = 'MASK'
    NO_TECHNIQUE = 'NO_TECHNIQUE'

    @staticmethod
    def values():
        return [n for n in list(AnonymizationTechnique.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class PrivacyType:
    IDENTIFIER = 'IDENTIFIER'
    QUASI_IDENTIFIER = 'QUASI_IDENTIFIER'
    SENSITIVE = 'SENSITIVE'
    NON_SENSITIVE = 'NON_SENSITIVE'

    @staticmethod
    def values():
        return [n for n in list(PrivacyType.__dict__.keys())
                if n[0] != '_' and n != 'values']


class Attribute(db.Model):
    """ Data source attribute. """
    __tablename__ = 'attribute'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(500))
    type = Column(Enum(*list(DataType.values()),
                       name='DataTypeEnumType'), nullable=False)
    size = Column(Integer)
    precision = Column(Integer)
    scale = Column(Integer)
    nullable = Column(Boolean,
                      default=False, nullable=False)
    enumeration = Column(Boolean,
                         default=False, nullable=False)
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
    deciles = Column(LONGTEXT)
    format = Column(String(100))

    # Associations
    data_source_id = Column(Integer,
                            ForeignKey("data_source.id"), nullable=False)
    data_source = relationship(
        "DataSource",
        foreign_keys=[data_source_id],
        backref=backref("attributes",
                        cascade="all, delete-orphan"))
    attribute_privacy = relationship(
        "AttributePrivacy", uselist=False,
        back_populates="attribute", lazy='joined')

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class AttributePrivacy(db.Model):
    """ Privacy configuration for an attribute. """
    __tablename__ = 'attribute_privacy'

    # Fields
    id = Column(Integer, primary_key=True)
    attribute_name = Column(String(200), nullable=False)
    data_type = Column(Enum(*list(DataType.values()),
                            name='DataTypeEnumType'))
    privacy_type = Column(Enum(*list(PrivacyType.values()),
                               name='PrivacyTypeEnumType'), nullable=False)
    category_technique = Column(String(100))
    anonymization_technique = Column(Enum(*list(AnonymizationTechnique.values()),
                                          name='AnonymizationTechniqueEnumType'), nullable=False)
    hierarchical_structure_type = Column(String(100))
    privacy_model_technique = Column(String(100))
    hierarchy = Column(LONGTEXT)
    category_model = Column(LONGTEXT)
    privacy_model = Column(LONGTEXT)
    privacy_model_parameters = Column(LONGTEXT)
    unlock_privacy_key = Column(String(400))
    is_global_law = Column(Boolean,
                           default=False)

    # Associations
    attribute_id = Column(Integer,
                          ForeignKey("attribute.id"))
    attribute = relationship(
        "Attribute",
        foreign_keys=[attribute_id],
        back_populates="attribute_privacy")
    attribute_privacy_group_id = Column(Integer,
                                        ForeignKey("attribute_privacy_group.id"))
    attribute_privacy_group = relationship(
        "AttributePrivacyGroup",
        foreign_keys=[attribute_privacy_group_id],
        backref=backref("attribute_privacy",
                        cascade="all, delete-orphan"))

    def __unicode__(self):
        return self.attribute_name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class AttributePrivacyGroup(db.Model):
    """ Groups attributes with same semantic """
    __tablename__ = 'attribute_privacy_group'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    user_id = Column(Integer, nullable=False)

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
    enabled = Column(Boolean,
                     default=True, nullable=False)
    statistics_process_counter = Column(Integer,
                                        default=0, nullable=False)
    read_only = Column(Boolean,
                       default=True, nullable=False)
    privacy_aware = Column(Boolean,
                           default=False, nullable=False)
    url = Column(String(200), nullable=False)
    created = Column(DateTime,
                     default=func.now(), nullable=False)
    updated = Column(DateTime,
                     default=datetime.datetime.utcnow, nullable=False,
                     onupdate=datetime.datetime.utcnow)
    format = Column(Enum(*list(DataSourceFormat.values()),
                         name='DataSourceFormatEnumType'), nullable=False)
    provenience = Column(LONGTEXT)
    estimated_rows = Column(Integer,
                            default=0)
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
    attribute_delimiter = Column(String(20))
    record_delimiter = Column(String(20))
    text_delimiter = Column(String(20))
    is_public = Column(Boolean,
                       default=False, nullable=False)
    treat_as_missing = Column(LONGTEXT)
    encoding = Column(String(200))
    is_first_line_header = Column(Boolean,
                                  default=0, nullable=False)
    is_multiline = Column(Boolean,
                          default=0, nullable=False)
    command = Column(LONGTEXT)
    __mapper_args__ = {
        'order_by': 'name'
    }

    # Associations
    storage_id = Column(Integer,
                        ForeignKey("storage.id"), nullable=False)
    storage = relationship(
        "Storage",
        foreign_keys=[storage_id])

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class DataSourcePermission(db.Model):
    """ Associate users and permissions """
    __tablename__ = 'data_source_permission'

    # Fields
    id = Column(Integer, primary_key=True)
    permission = Column(Enum(*list(PermissionType.values()),
                             name='PermissionTypeEnumType'), nullable=False)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)

    # Associations
    data_source_id = Column(Integer,
                            ForeignKey("data_source.id"), nullable=False)
    data_source = relationship(
        "DataSource",
        foreign_keys=[data_source_id],
        backref=backref("permissions",
                        cascade="all, delete-orphan"))

    def __unicode__(self):
        return self.permission

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class Model(db.Model):
    """ Machine learning model """
    __tablename__ = 'model'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    enabled = Column(Boolean,
                     default=True, nullable=False)
    created = Column(DateTime,
                     default=func.now(), nullable=False)
    path = Column(String(500), nullable=False)
    class_name = Column(String(500), nullable=False)
    type = Column(Enum(*list(ModelType.values()),
                       name='ModelTypeEnumType'),
                  default=ModelType.UNSPECIFIED, nullable=False)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)
    workflow_id = Column(Integer, nullable=False)
    workflow_name = Column(String(200))
    task_id = Column(String(200), nullable=False)
    job_id = Column(Integer, nullable=False)

    # Associations
    storage_id = Column(Integer,
                        ForeignKey("storage.id"), nullable=False)
    storage = relationship(
        "Storage",
        foreign_keys=[storage_id])

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class ModelPermission(db.Model):
    """ Associate users and permissions to models """
    __tablename__ = 'model_permission'

    # Fields
    id = Column(Integer, primary_key=True)
    permission = Column(Enum(*list(PermissionType.values()),
                             name='PermissionTypeEnumType'), nullable=False)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)

    # Associations
    model_id = Column(Integer,
                      ForeignKey("model.id"), nullable=False)
    model = relationship(
        "Model",
        foreign_keys=[model_id],
        backref=backref("permissions",
                        cascade="all, delete-orphan"))

    def __unicode__(self):
        return self.permission

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class PrivacyRisk(db.Model):
    """ Privacy information associated to the data source """
    __tablename__ = 'privacy_risk'

    # Fields
    id = Column(Integer, primary_key=True)
    type = Column(Enum(*list(PrivacyRiskType.values()),
                       name='PrivacyRiskTypeEnumType'), nullable=False)
    probability = Column(Float)
    impact = Column(Float)
    value = Column(Float, nullable=False)
    detail = Column(LONGTEXT, nullable=False)

    # Associations
    data_source_id = Column(Integer,
                            ForeignKey("data_source.id"), nullable=False)
    data_source = relationship(
        "DataSource",
        foreign_keys=[data_source_id],
        backref=backref("risks",
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
    type = Column(Enum(*list(StorageType.values()),
                       name='StorageTypeEnumType'), nullable=False)
    enabled = Column(Boolean,
                     default=True, nullable=False)
    url = Column(String(1000), nullable=False)

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class StoragePermission(db.Model):
    """ Associate users and permissions """
    __tablename__ = 'storage_permission'

    # Fields
    id = Column(Integer, primary_key=True)
    permission = Column(Enum(*list(PermissionType.values()),
                             name='PermissionTypeEnumType'), nullable=False)
    user_id = Column(Integer, nullable=False)

    # Associations
    storage_id = Column(Integer,
                        ForeignKey("storage.id"), nullable=False)
    storage = relationship(
        "Storage",
        foreign_keys=[storage_id],
        backref=backref("permissions",
                        cascade="all, delete-orphan"))

    def __unicode__(self):
        return self.permission

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)

