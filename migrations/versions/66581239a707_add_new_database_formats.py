"""add_types

Revision ID: 66581239a707

"""

from alembic import context
from sqlalchemy.orm import sessionmaker
from limonero.migration_utils import (downgrade_actions, upgrade_actions,
        is_mysql, get_psql_enum_alter_commands, is_psql, is_sqlite)

# revision identifiers, used by Alembic.
revision = '66581239a707'
down_revision = '88672039047e'
branch_labels = None
depends_on = None


def get_commands():
    values = [
        'CSV', 'CUSTOM', 'GEO_JSON', 'JDBC', 'IMAGE_FOLDER', 'DATA_FOLDER', 
        'HAR_IMAGE_FOLDER', 'HDF5', 'HIVE', 'JSON', 'NPY', 'PICKLE', 'PARQUET',
        'SAV, SHAPEFILE', 'TAR_IMAGE_FOLDER', 'TEXT', 'VIDEO_FOLDER',
        'XML_FILE', 'UNKNOWN'
    ]
    values_str = ', '.join([f"'{v}'" for v in values])
    if is_mysql():
        all_commands = [
            [f'''
                ALTER TABLE data_source CHANGE `format` `format`
                ENUM({values_str}) CHARSET utf8 COLLATE utf8_unicode_ci
                NOT NULL; ''',
             f'''
                 ALTER TABLE data_source CHANGE `format` `format`
                ENUM({values_str}) CHARSET utf8 COLLATE utf8_unicode_ci
                NOT NULL;
             ''']]
    elif is_psql():
        all_commands = [
            [
               get_psql_enum_alter_commands(['data_source'], ['format'], 
                   'DataSourceFormatEnumType', values, 'CSV'),
               get_psql_enum_alter_commands(['data_source'], ['format'], 
                   'DataSourceFormatEnumType', values, 'CSV'),
             ]]
    elif is_sqlite():
        all_commands = [
            [
                [], []
             ]]

    return all_commands

def upgrade():
    upgrade_actions(get_commands())

def downgrade():
    downgrade_actions(get_commands())
