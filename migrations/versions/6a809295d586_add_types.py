"""add_types

Revision ID: 6a809295d586
Revises: 0eadac2ac523
Create Date: 2018-05-04 19:37:29.033365

"""

from alembic import context
from sqlalchemy.orm import sessionmaker
from limonero.migration_utils import (downgrade_actions, upgrade_actions,
        get_engine_name, get_psql_enum_alter_commands)

# revision identifiers, used by Alembic.
revision = '6a809295d586'
down_revision = '0eadac2ac523'
branch_labels = None
depends_on = None


def get_commands():
    if get_engine_name() == 'mysql':
        all_commands = [
            ['''
                ALTER TABLE data_source CHANGE `format` `format`
                ENUM('XML_FILE','NETCDF4','HDF5','SHAPEFILE','TEXT','CUSTOM','JSON',
                'CSV','PICKLE','GEO_JSON') CHARSET utf8 COLLATE utf8_unicode_ci
                NOT NULL; ''',
             '''
                 ALTER TABLE data_source CHANGE `format` `format`
                ENUM('XML_FILE','NETCDF4','HDF5','SHAPEFILE','TEXT','CUSTOM','JSON',
                'CSV','PICKLE','GEO_JSON') CHARSET utf8 COLLATE utf8_unicode_ci
                NOT NULL;
             ''']]
    else:
        values = ['XML_FILE','NETCDF4','HDF5','SHAPEFILE','TEXT','CUSTOM','JSON',
                            'CSV','PICKLE','GEO_JSON']
        all_commands = [
            [
               get_psql_enum_alter_commands(['data_source'], ['format'], 
                   'DataSourceFormatEnumType', values, 'CSV'),
               get_psql_enum_alter_commands(['data_source'], ['format'], 
                   'DataSourceFormatEnumType', values, 'CSV'),
             ]]
    return all_commands

def upgrade():
    upgrade_actions(get_commands())

def downgrade():
    downgrade_actions(get_commands())
