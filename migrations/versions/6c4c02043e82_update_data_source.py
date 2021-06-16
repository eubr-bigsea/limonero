"""Update data source

Revision ID: 6c4c02043e82
Revises: 55f14e046318
Create Date: 2018-09-05 16:08:47.192237

"""

from alembic import context
from sqlalchemy.orm import sessionmaker
from limonero.migration_utils import (get_enable_disable_fk_command,
        upgrade_actions, downgrade_actions, get_psql_enum_alter_commands,
        is_mysql)

# revision identifiers, used by Alembic.
revision = '6c4c02043e82'
down_revision = '55f14e046318'
branch_labels = None
depends_on = None

def get_commands():
    if is_mysql():
        all_commands = [
            ['''
                ALTER TABLE data_source CHANGE `format` `format`
                ENUM('CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
                'PARQUET','PICKLE','SHAPEFILE','TEXT','UNKNOWN', 'XML_FILE')
                CHARSET utf8 COLLATE utf8_unicode_ci
                NOT NULL; ''',
             '''
                 ALTER TABLE data_source CHANGE `format` `format`
                 ENUM('CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
                'PICKLE','SHAPEFILE','TEXT','XML_FILE')
                CHARSET utf8 COLLATE utf8_unicode_ci
                NOT NULL;
             '''],
            ["""
                ALTER TABLE data_source CHANGE `task_id` `task_id`
                VARCHAR(255) NULL; """,
             """ALTER TABLE data_source CHANGE `task_id` `task_id`
             INT NULL; """,
             ],
            ["""
            ALTER TABLE attribute CHANGE `type` `type`
                ENUM('BINARY','CHARACTER','DECIMAL','DATE','DATETIME','DOUBLE','ENUM',
                'FLOAT','INTEGER','LAT_LONG','LONG','TEXT','TIME','VECTOR','TIMESTAMP')
                CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;
            """,
             """
             ALTER TABLE attribute CHANGE `type` `type`
                ENUM('BINARY','CHARACTER','DECIMAL','DATE','DATETIME','DOUBLE','ENUM',
                'FLOAT','INTEGER','LAT_LONG','LONG','TEXT','TIME','VECTOR','TIMESTAMP')
                CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL; """]
        ]
    else:
        new_ds_values = ['CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
                'PARQUET','PICKLE','SHAPEFILE','TEXT','UNKNOWN', 'XML_FILE']
        old_ds_values = ['CSV','CUSTOM','GEO_JSON','HDF5','JDBC','JSON','NETCDF4',
                'PICKLE','SHAPEFILE','TEXT','XML_FILE']
        new_attr_values = ['BINARY','CHARACTER','DECIMAL','DATE','DATETIME','DOUBLE','ENUM',
                'FLOAT','INTEGER','LAT_LONG','LONG','TEXT','TIME','VECTOR','TIMESTAMP']
        old_attr_values = ['BINARY','CHARACTER','DECIMAL','DATE','DATETIME','DOUBLE','ENUM',
                'FLOAT','INTEGER','LAT_LONG','LONG','TEXT','TIME','VECTOR','TIMESTAMP']

        all_commands = [
            [
                get_psql_enum_alter_commands(['data_source'], ['format'], 
                    'DataSourceFormatEnumType', new_ds_values, 'CSV'),
                get_psql_enum_alter_commands(['data_source'], ['format'], 
                    'DataSourceFormatEnumType', old_ds_values, 'CSV'),
             ],
            [
                'ALTER TABLE data_source ALTER COLUMN task_id TYPE VARCHAR(255)',
                'ALTER TABLE data_source ALTER COLUMN task_id TYPE INT USING (task_id::integer)',
             ],
            [
                get_psql_enum_alter_commands(
                    ['attribute', 'attribute_privacy'], ['type', 'data_type'], 
                    'DataTypeEnumType', new_attr_values, 'INTEGER'),
                get_psql_enum_alter_commands(
                    ['attribute', 'attribute_privacy'], ['type', 'data_type'], 
                    'DataTypeEnumType', old_attr_values, 'INTEGER'),
             ]
        ]
    return all_commands


def upgrade():
    upgrade_actions(get_commands())


def downgrade():
    downgrade_actions(get_commands())
