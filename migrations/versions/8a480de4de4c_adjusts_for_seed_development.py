"""Adjusts for Seed development

Revision ID: 8a480de4de4c
Revises: 7addb7587b1a
Create Date: 2021-07-13 17:16:20.807567

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from limonero.migration_utils import (is_mysql, is_psql, upgrade_actions, 
        downgrade_actions, get_psql_enum_alter_commands, is_sqlite)



# revision identifiers, used by Alembic.
revision = '8a480de4de4c'
down_revision = '7addb7587b1a'
branch_labels = None
depends_on = None

def upgrade():
    if is_mysql():
        op.execute("""
            ALTER TABLE `storage` CHANGE `type` `type` ENUM(
                'CASSANDRA','ELASTIC_SEARCH','HDFS','HIVE', 'HIVE_WAREHOUSE',
                 'JDBC', 'KAFKA', 'LOCAL','MONGODB'
                ) CHARSET utf8 COLLATE
                utf8_unicode_ci NOT NULL;""")
    elif is_psql():
        storage_values = ['CASSANDRA','ELASTIC_SEARCH','HDFS',
                'HIVE', 'HIVE_WAREHOUSE', 'JDBC', 'KAFKA', 'LOCAL','MONGODB']
        all_commands = [
            [
                get_psql_enum_alter_commands(['storage'], ['type'],
                    'StorageTypeEnumType', storage_values, 'HDFS'),
                None
            ]
        ]
        upgrade_actions(all_commands)
    # ### end Alembic commands ###


def downgrade():
    if is_mysql():
        op.execute("""
            ALTER TABLE `storage` CHANGE `type` `type` ENUM(
                'CASSANDRA','ELASTIC_SEARCH','HDFS','HIVE', 'HIVE_WAREHOUSE',
                 'KAFKA', 'JDBC','LOCAL','MONGODB'
                ) CHARSET utf8 COLLATE
                utf8_unicode_ci NOT NULL;""")
    elif is_psql():
        storage_values = ['CASSANDRA','ELASTIC_SEARCH','HDFS',
                'HIVE', 'HIVE_WAREHOUSE', 'JDBC','LOCAL','MONGODB']
        all_commands = [
            [
                None,
                get_psql_enum_alter_commands(['storage'], ['type'],
                    'StorageTypeEnumType', storage_values, 'HDFS'),
            ]
        ]
        downgrade_actions(all_commands)
