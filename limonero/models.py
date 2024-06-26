import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float, \
    Enum, DateTime, Numeric, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref

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
    HIVE = 'HIVE'
    JSON = 'JSON'
    NPY = 'NPY'
    PICKLE = 'PICKLE'
    PARQUET = 'PARQUET'
    SAV = 'SAV'
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
class DataSourceInitialization:
    NO_INITIALIZED = 'NO_INITIALIZED'
    INITIALIZING = 'INITIALIZING'
    INITIALIZED = 'INITIALIZED'

    @staticmethod
    def values():
        return [n for n in list(DataSourceInitialization.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class ModelType:
    KERAS = 'KERAS'
    MLEAP = 'MLEAP'
    PERFORMANCE_SPARK = 'PERFORMANCE_SPARK'
    PERFORMANCE_KERAS = 'PERFORMANCE_KERAS'
    SPARK_ML_CLASSIFICATION = 'SPARK_ML_CLASSIFICATION'
    SPARK_ML_REGRESSION = 'SPARK_ML_REGRESSION'
    SPARK_MLLIB_CLASSIFICATION = 'SPARK_MLLIB_CLASSIFICATION'
    UNSPECIFIED = 'UNSPECIFIED'

    @staticmethod
    def values():
        return [n for n in list(ModelType.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class DeploymentStatus:
    NOT_DEPLOYED = 'NOT_DEPLOYED'
    ERROR = 'ERROR'
    EDITING = 'EDITING'
    SAVED = 'SAVED'
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'
    SUSPENDED = 'SUSPENDED'
    PENDING = 'PENDING'
    DEPLOYED = 'DEPLOYED'

    @staticmethod
    def values():
        return [n for n in list(DeploymentStatus.__dict__.keys())
                if n[0] != '_' and n != 'values']


# noinspection PyClassHasNoInit
class StorageType:
    MONGODB = 'MONGODB'
    ELASTIC_SEARCH = 'ELASTIC_SEARCH'
    HDFS = 'HDFS'
    HIVE = 'HIVE'
    HIVE_WAREHOUSE = 'HIVE_WAREHOUSE'
    KAFKA = 'KAFKA'
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
    FILE = 'FILE'
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
class AttributeForeignKeyDirection:
    FROM = 'FROM'
    TO = 'TO'

    @staticmethod
    def values():
        return [n for n in list(AttributeForeignKeyDirection.__dict__.keys())
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

# Association tables definition


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
    deciles = Column(Text(4294000000))
    format = Column(String(100))
    key = Column(Boolean,
                 default=False, nullable=False)

    # Associations
    data_source_id = Column(
        Integer,
        ForeignKey("data_source.id",
                   name="fk_attribute_data_source_id"),
        nullable=False,
        index=True)
    data_source = relationship(
        "DataSource",
        overlaps='attributes',
        foreign_keys=[data_source_id],
        backref=backref("attributes",
                        cascade="all, delete-orphan"))
    attribute_privacy = relationship(
        "AttributePrivacy", uselist=False,
        back_populates="attribute", lazy='joined')

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class AttributeForeignKey(db.Model):
    """ Attribute that form a foreign key in data sources """
    __tablename__ = 'attribute_foreign_key'

    # Fields
    id = Column(Integer, primary_key=True)
    order = Column(Integer, nullable=False)
    direction = Column(Enum(*list(AttributeForeignKeyDirection.values()),
                            name='AttributeForeignKeyDirectionEnumType'), nullable=False)

    # Associations
    foreign_key_id = Column(
        Integer,
        ForeignKey("data_source_foreign_key.id",
                   name="fk_attribute_foreign_key_foreign_key_id"),
        nullable=False,
        index=True)
    foreign_key = relationship(
        "DataSourceForeignKey",
        overlaps='attributes',
        foreign_keys=[foreign_key_id],
        backref=backref("attributes",
                        cascade="all, delete-orphan"))
    from_attribute_id = Column(
        Integer,
        ForeignKey("attribute.id",
                   name="fk_attribute_foreign_key_from_attribute_id"),
        nullable=False,
        index=True)
    from_attribute = relationship(
        "Attribute",
        overlaps='foreign_keys',
        foreign_keys=[from_attribute_id],
        backref=backref("foreign_keys",
                        cascade="all, delete-orphan"))
    to_attribute_id = Column(
        Integer,
        ForeignKey("attribute.id",
                   name="fk_attribute_foreign_key_to_attribute_id"),
        nullable=False,
        index=True)
    to_attribute = relationship(
        "Attribute",
        overlaps='references',
        foreign_keys=[to_attribute_id],
        backref=backref("references",
                        cascade="all, delete-orphan"))

    def __str__(self):
        return self.order

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
    hierarchy = Column(Text(4294000000))
    category_model = Column(Text(4294000000))
    privacy_model = Column(Text(4294000000))
    privacy_model_parameters = Column(Text(4294000000))
    unlock_privacy_key = Column(String(400))
    is_global_law = Column(Boolean,
                           default=False)

    # Associations
    attribute_id = Column(
        Integer,
        ForeignKey("attribute.id",
                   name="fk_attribute_privacy_attribute_id"),
        index=True)
    attribute = relationship(
        "Attribute",
        overlaps='attribute_privacy',
        foreign_keys=[attribute_id],
        back_populates="attribute_privacy")
    attribute_privacy_group_id = Column(
        Integer,
        ForeignKey("attribute_privacy_group.id",
                   name="fk_attribute_privacy_attribute_privacy_group_id"),
        index=True)
    attribute_privacy_group = relationship(
        "AttributePrivacyGroup",
        overlaps='attribute_privacy',
        foreign_keys=[attribute_privacy_group_id],
        backref=backref("attribute_privacy",
                        cascade="all, delete-orphan"))

    def __str__(self):
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

    def __str__(self):
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
    initialization = Column(Enum(*list(DataSourceInitialization.values()),
                                 name='DataSourceInitializationEnumType'),
                            default=DataSourceInitialization.INITIALIZED, nullable=False)
    initialization_job_id = Column(String(200))
    provenience = Column(Text(4294000000))
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
    workflow_id = Column(Integer, index=True)
    workflow_version = Column(Integer)
    task_id = Column(String(200), index=True)
    attribute_delimiter = Column(String(20))
    record_delimiter = Column(String(20))
    text_delimiter = Column(String(20))
    is_public = Column(Boolean,
                       default=False, nullable=False)
    treat_as_missing = Column(Text(4294000000))
    encoding = Column(String(200))
    is_first_line_header = Column(Boolean,
                                  default=0, nullable=False)
    is_multiline = Column(Boolean,
                          default=0, nullable=False)
    command = Column(Text(4294000000))
    is_lookup = Column(Boolean,
                       default=0, nullable=False)
    use_in_workflow = Column(Boolean,
                             default=0, nullable=False, index=True)

    # Associations
    storage_id = Column(
        Integer,
        ForeignKey("storage.id",
                   name="fk_data_source_storage_id"),
        nullable=False,
        index=True)
    storage = relationship(
        "Storage",
        overlaps='storage',
        foreign_keys=[storage_id])

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class DataSourceForeignKey(db.Model):
    """ Foreign key in data sources """
    __tablename__ = 'data_source_foreign_key'

    # Fields
    id = Column(Integer, primary_key=True)

    # Associations
    from_source_id = Column(
        Integer,
        ForeignKey("data_source.id",
                   name="fk_data_source_foreign_key_from_source_id"),
        nullable=False,
        index=True)
    from_source = relationship(
        "DataSource",
        overlaps='foreign_keys',
        foreign_keys=[from_source_id],
        backref=backref("foreign_keys",
                        cascade="all, delete-orphan"))
    to_source_id = Column(
        Integer,
        ForeignKey("data_source.id",
                   name="fk_data_source_foreign_key_to_source_id"),
        nullable=False,
        index=True)
    to_source = relationship(
        "DataSource",
        overlaps='references',
        foreign_keys=[to_source_id],
        backref=backref("references",
                        cascade="all, delete-orphan"))

    def __str__(self):
        return 'DataSourceForeignKey'

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
    data_source_id = Column(
        Integer,
        ForeignKey("data_source.id",
                   name="fk_data_source_permission_data_source_id"),
        nullable=False,
        index=True)
    data_source = relationship(
        "DataSource",
        overlaps='permissions',
        foreign_keys=[data_source_id],
        backref=backref("permissions",
                        cascade="all, delete-orphan"))

    def __str__(self):
        return self.permission

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)


class DataSourceVariable(db.Model):
    """ Variables for data source """
    __tablename__ = 'data_source_variable'

    # Fields
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    label = Column(String(200))
    description = Column(Text(4294000000))
    default_value = Column(Text(4294000000))

    # Associations
    data_source_id = Column(
        Integer,
        ForeignKey("data_source.id",
                   name="fk_data_source_variable_data_source_id"),
        nullable=False,
        index=True)
    data_source = relationship(
        "DataSource",
        overlaps='variables',
        foreign_keys=[data_source_id],
        backref=backref("variables",
                        cascade="all, delete-orphan"))

    def __str__(self):
        return self.name

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
    deployment_status = Column(Enum(*list(DeploymentStatus.values()),
                                    name='DeploymentStatusEnumType'),
                               default=DeploymentStatus.NOT_DEPLOYED, nullable=False)
    user_id = Column(Integer, nullable=False)
    user_login = Column(String(50), nullable=False)
    user_name = Column(String(200), nullable=False)
    workflow_id = Column(Integer)
    workflow_name = Column(String(200))
    task_id = Column(String(200))
    job_id = Column(Integer)

    # Associations
    storage_id = Column(
        Integer,
        ForeignKey("storage.id",
                   name="fk_model_storage_id"),
        nullable=False,
        index=True)
    storage = relationship(
        "Storage",
        overlaps='storage',
        foreign_keys=[storage_id])

    def __str__(self):
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
    model_id = Column(
        Integer,
        ForeignKey("model.id",
                   name="fk_model_permission_model_id"),
        nullable=False,
        index=True)
    model = relationship(
        "Model",
        overlaps='permissions',
        foreign_keys=[model_id],
        backref=backref("permissions",
                        cascade="all, delete-orphan"))

    def __str__(self):
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
    detail = Column(Text(4294000000), nullable=False)

    # Associations
    data_source_id = Column(
        Integer,
        ForeignKey("data_source.id",
                   name="fk_privacy_risk_data_source_id"),
        nullable=False,
        index=True)
    data_source = relationship(
        "DataSource",
        overlaps='risks',
        foreign_keys=[data_source_id],
        backref=backref("risks",
                        cascade="all, delete-orphan"))

    def __str__(self):
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
    client_url = Column(String(1000))
    extra_params = Column(Text(4294000000))

    def __str__(self):
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
    storage_id = Column(
        Integer,
        ForeignKey("storage.id",
                   name="fk_storage_permission_storage_id"),
        nullable=False,
        index=True)
    storage = relationship(
        "Storage",
        overlaps='permissions',
        foreign_keys=[storage_id],
        backref=backref("permissions",
                        cascade="all, delete-orphan"))

    def __str__(self):
        return self.permission

    def __repr__(self):
        return '<Instance {}: {}>'.format(self.__class__, self.id)

