"""Add new model types

Revision ID: 96581239a707
Revises: 32053847c4db
Create Date: 2019-08-30 08:51:32.168436

"""
from alembic import context
from sqlalchemy.orm import sessionmaker
from limonero.migration_utils import (get_enable_disable_fk_command,
        upgrade_actions, downgrade_actions, get_psql_enum_alter_commands,
        is_mysql, is_psql)

# revision identifiers, used by Alembic.
revision = '96581239a707'
down_revision = '32053847c4db'
branch_labels = None
depends_on = None

def get_commands():
    all_commands = []
    if is_mysql():
        all_commands = [
            ['''
                ALTER TABLE `model` CHANGE `type` `type`
                ENUM('KERAS','PERFORMANCE_SPARK',
                'PERFORMANCE_KERAS', 'PERFORMANCE',
                'SPARK_ML_REGRESSION',
                'SPARK_MLLIB_CLASSIFICATION',
                'SPARK_ML_CLASSIFICATION','UNSPECIFIED')
                CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;  ''',
             '''
                ALTER TABLE `model` CHANGE `type` `type`
                ENUM('KERAS', 'SPARK_ML_REGRESSION', 'PERFORMANCE',
                'SPARK_MLLIB_CLASSIFICATION',
                'SPARK_ML_CLASSIFICATION','UNSPECIFIED')
                CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;  '''],
            #['ALTER TABLE `model` ADD INDEX `inx_type` (`type`);',
            # 'ALTER TABLE `model` DROP INDEX `inx_type`;'],
            [
                """
                ALTER TABLE `model`
                    CHANGE `job_id` `job_id` INT(11) NULL,
                    CHANGE `task_id` `task_id` VARCHAR(200)
                        CHARSET utf8 COLLATE utf8_unicode_ci NULL,
                    CHANGE `workflow_id` `workflow_id` INT(11) NULL;
                """,
                """
                ALTER TABLE `model`
                    CHANGE `job_id` `job_id` INT(11),
                    CHANGE `task_id` `task_id` VARCHAR(200)
                        CHARSET utf8 COLLATE utf8_unicode_ci, 
                    CHANGE `workflow_id` `workflow_id` INT(11);"""
            ],
            ["""
                ALTER TABLE `storage` CHANGE `type` `type` ENUM(
                    'CASSANDRA','ELASTIC_SEARCH','HBASE','HDFS','JDBC','LOCAL',
                    'MONGODB','OPHIDIA','POSTGIS') CHARSET utf8 COLLATE
                    utf8_unicode_ci NOT NULL;""",
             """
             ALTER TABLE `storage` CHANGE `type` `type` ENUM(
                    'CASSANDRA','ELASTIC_SEARCH','HBASE','HDFS','JDBC','LOCAL',
                    'MONGODB','OPHIDIA','POSTGIS') CHARSET utf8 COLLATE
                    utf8_unicode_ci NOT NULL;""",
             ],
            [
                """
                ALTER TABLE `data_source` CHANGE `format` `format`
                    ENUM('CSV','CUSTOM','GEO_JSON','HAR_IMAGE_FOLDER','HDF5',
                    'DATA_FOLDER','IMAGE_FOLDER','JDBC','JSON','NETCDF4','PARQUET',
                    'PICKLE','SHAPEFILE','TAR_IMAGE_FOLDER','TEXT','VIDEO_FOLDER',
                    'UNKNOWN', 'XML_FILE') CHARSET utf8
                    COLLATE utf8_unicode_ci NOT NULL;
                """,
                """
                ALTER TABLE `data_source` CHANGE `format` `format`
                    ENUM('CSV','CUSTOM','GEO_JSON','HAR_IMAGE_FOLDER','HDF5',
                    'DATA_FOLDER','IMAGE_FOLDER','JDBC','JSON','NETCDF4','PARQUET',
                    'PICKLE','SHAPEFILE','TAR_IMAGE_FOLDER','TEXT','VIDEO_FOLDER',
                    'UNKNOWN','XML_FILE') CHARSET utf8
                    COLLATE utf8_unicode_ci NOT NULL;
                """
        
            ]
        ]
    elif is_psql():
        new_ds_values = ['CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
                'PARQUET','PICKLE','SHAPEFILE','TEXT','UNKNOWN', 'XML_FILE']
        old_ds_values = ['CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
                'PICKLE','SHAPEFILE','TEXT','XML_FILE']

        new_model_values = ['KERAS','PERFORMANCE_SPARK',
                'PERFORMANCE_KERAS', 'PERFORMANCE',
                'SPARK_ML_REGRESSION',
                'SPARK_MLLIB_CLASSIFICATION',
                'SPARK_ML_CLASSIFICATION','UNSPECIFIED']
        old_model_values = ['KERAS', 'SPARK_ML_REGRESSION', 'PERFORMANCE',
                'SPARK_MLLIB_CLASSIFICATION',
                'SPARK_ML_CLASSIFICATION','UNSPECIFIED']
        new_storage_values = ['CASSANDRA','ELASTIC_SEARCH','HBASE','HDFS','JDBC','LOCAL',
                    'MONGODB','OPHIDIA','POSTGIS']
        old_storage_values = ['CASSANDRA','ELASTIC_SEARCH','HBASE','HDFS','JDBC','LOCAL',
                    'MONGODB','OPHIDIA','POSTGIS']

        all_commands = [
            [
                get_psql_enum_alter_commands(['model'], ['type'], 
                    'ModelTypeEnumType', new_model_values, 'UNSPECIFIED'),
                get_psql_enum_alter_commands(['model'], ['type'], 
                    'ModelTypeEnumType', old_model_values, 'UNSPECIFIED'),
            ],
            [
                """
                ALTER TABLE model
                    ALTER COLUMN job_id DROP NOT NULL,
                    ALTER COLUMN task_id DROP NOT NULL,
                    ALTER COLUMN workflow_id DROP NOT NULL;
                """,
                """
                 ALTER TABLE model
                    ALTER COLUMN job_id DROP NOT NULL,
                    ALTER COLUMN task_id DROP NOT NULL,
                    ALTER COLUMN workflow_id DROP NOT NULL;
                """
            ],
            [
                get_psql_enum_alter_commands(['storage'], ['type'], 
                    'StorageTypeEnumType', new_storage_values, 'HDFS'),
                get_psql_enum_alter_commands(['storage'], ['type'], 
                    'StorageTypeEnumType', old_storage_values, 'HDFS'),

            ],
            [
                get_psql_enum_alter_commands(['data_source'], ['format'], 
                    'DataSourceFormatEnumType', new_ds_values, 'CSV'),
                get_psql_enum_alter_commands(['data_source'], ['format'], 
                    'DataSourceFormatEnumType', old_ds_values, 'CSV'),
            ]
        ]
    return all_commands

def upgrade():
    upgrade_actions(get_commands())

def downgrade():
    downgrade_actions(get_commands())
