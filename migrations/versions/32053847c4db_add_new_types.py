"""Add new types

Revision ID: 32053847c4db
Revises: 05a62958a9cc
Create Date: 2019-06-11 10:36:14.456629

"""

from alembic import context
from sqlalchemy.orm import sessionmaker
from limonero.migration_utils import (get_enable_disable_fk_command,
        upgrade_actions, downgrade_actions, get_psql_enum_alter_commands,
        is_mysql)
# revision identifiers, used by Alembic.
revision = '32053847c4db'
down_revision = '05a62958a9cc'
branch_labels = None
depends_on = None

def get_commands():
    if is_mysql():
        all_commands = [
            (""" ALTER TABLE data_source CHANGE `format` `format` ENUM(
                'CSV','CUSTOM','GEO_JSON','HAR_IMAGE_FOLDER','HDF5','DATA_FOLDER',
                'IMAGE_FOLDER', 'JDBC','JSON','NETCDF4','PARQUET','PICKLE','SHAPEFILE',
                'TAR_IMAGE_FOLDER','TEXT', 'VIDEO_FOLDER',
                'UNKNOWN','XML_FILE') CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;""",
             """ ALTER TABLE data_source CHANGE `format` `format` ENUM(
                'CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON',
                'NETCDF4','PARQUET','PICKLE','SHAPEFILE','TEXT',
                'UNKNOWN','XML_FILE') CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;"""
             ),
            ("""
                ALTER TABLE `storage` CHANGE `type` `type` ENUM(
                    'HDFS','OPHIDIA','ELASTIC_SEARCH','MONGODB','POSTGIS','HBASE',
                    'CASSANDRA','JDBC','LOCAL') CHARSET utf8 COLLATE
                    utf8_unicode_ci NOT NULL;""",
             """
             ALTER TABLE `storage` CHANGE `type` `type` ENUM(
                'HDFS','OPHIDIA','ELASTIC_SEARCH','MONGODB','POSTGIS','HBASE',
                'CASSANDRA','JDBC') CHARSET utf8 COLLATE
                utf8_unicode_ci NOT NULL;""",
             ),
            (
                """ALTER TABLE `model` CHANGE `type` `type` ENUM(
                    'KERAS','SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION',
                    'SPARK_ML_CLASSIFICATION','UNSPECIFIED')
                    CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL; """,
                """ALTER TABLE `model` CHANGE `type` `type` ENUM(
                    'KERAS','SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION',
                    'SPARK_ML_CLASSIFICATION','UNSPECIFIED')
                    CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL; """
            )
        ]
    else:
        old_ds_values = ['CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
                'PARQUET','PICKLE','SHAPEFILE','TEXT','UNKNOWN', 'XML_FILE']
        new_ds_values = ['CSV','CUSTOM','GEO_JSON','HAR_IMAGE_FOLDER','HDF5','DATA_FOLDER',
                'IMAGE_FOLDER', 'JDBC','JSON','NETCDF4','PARQUET','PICKLE','SHAPEFILE',
                'TAR_IMAGE_FOLDER','TEXT', 'VIDEO_FOLDER',
                'UNKNOWN','XML_FILE']

        new_model_values = ['KERAS','SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION',
                    'SPARK_ML_CLASSIFICATION','UNSPECIFIED']
        old_model_values = ['KERAS','SPARK_ML_REGRESSION','SPARK_MLLIB_CLASSIFICATION',
                    'SPARK_ML_CLASSIFICATION','UNSPECIFIED']

        new_storage_values = [
                'HDFS','OPHIDIA','ELASTIC_SEARCH','MONGODB','POSTGIS','HBASE',
                'CASSANDRA','JDBC','LOCAL']
        old_storage_values = [
                'HDFS','OPHIDIA','ELASTIC_SEARCH','MONGODB','POSTGIS','HBASE',
                'CASSANDRA','JDBC']

        all_commands = [
            [
                get_psql_enum_alter_commands(['data_source'], ['format'], 
                    'DataSourceFormatEnumType', new_ds_values, 'CSV'),
                get_psql_enum_alter_commands(['data_source'], ['format'], 
                    'DataSourceFormatEnumType', old_ds_values, 'CSV'),
             ],
            [
                get_psql_enum_alter_commands(['model'], ['type'], 
                    'ModelTypeEnumType', new_model_values, 'UNSPECIFIED'),
                get_psql_enum_alter_commands(['model'], ['type'], 
                    'ModelTypeEnumType', old_model_values, 'UNSPECIFIED'),
             ],
            [
                get_psql_enum_alter_commands(['storage'], ['type'], 
                    'StorageTypeEnumType', new_storage_values, 'HDFS'),
                get_psql_enum_alter_commands(['storage'], ['type'], 
                    'StorageTypeEnumType', old_storage_values, 'HDFS'),
             ]
        ]

    return all_commands

def upgrade():
    upgrade_actions(get_commands())

def downgrade():
    downgrade_actions(get_commands())
