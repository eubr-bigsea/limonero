"""Add new types

Revision ID: 82053847c4db
Revises: b0815c138c24

"""

from alembic import context
from sqlalchemy.orm import sessionmaker
from limonero.migration_utils import (get_enable_disable_fk_command,
        upgrade_actions, downgrade_actions, get_psql_enum_alter_commands,
        is_mysql, is_psql)
from limonero.models import StorageType, DataSourceFormat
# revision identifiers, used by Alembic.
revision = '82053847c4db'
down_revision = 'b0815c138c24'
branch_labels = None
depends_on = None

def get_commands():
    all_commands = []

    def prepare(values):
        return ','.join([f"'{v}'" for v in values])
    
    formats = DataSourceFormat.values()
    old_formats = [f for f in DataSourceFormat.values() if f not in ('ICEBERG')]
    storage_types = StorageType.values()
    old_storage_types = [t for t in StorageType.values() if t not in ('S3', 'ICEBERG_CATALOG')]
    if is_mysql():
        all_commands = [
            (
            f""" ALTER TABLE data_source CHANGE `format` `format` 
            ENUM({prepare(formats)}) CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;""",
            f""" ALTER TABLE data_source CHANGE `format` `format` 
            ENUM({prepare(old_formats)}) CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;""",
             ),
            (
                f""" ALTER TABLE `storage` CHANGE `type` `type` 
                ENUM({prepare(storage_types)}) CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;""",
                f""" ALTER TABLE `storage` CHANGE `type` `type` 
                ENUM({prepare(old_storage_types)}) CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;""",
             ),
        ]
    elif is_psql():
        all_commands = [
            [
                get_psql_enum_alter_commands(['data_source'], ['format'], 
                    'DataSourceFormatEnumType', formats, 'CSV'),
                get_psql_enum_alter_commands(['data_source'], ['format'], 
                    'DataSourceFormatEnumType', old_formats, 'CSV'),
             ],
            [
                get_psql_enum_alter_commands(['storage'], ['type'], 
                    'StorageTypeEnumType', storage_types, 'HDFS'),
                get_psql_enum_alter_commands(['storage'], ['type'], 
                    'StorageTypeEnumType', old_storage_types, 'HDFS'),
             ]
        ]

    return all_commands

def upgrade():
    upgrade_actions(get_commands())

def downgrade():
    downgrade_actions(get_commands())
