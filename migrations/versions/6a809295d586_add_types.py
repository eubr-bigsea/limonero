"""add_types

Revision ID: 6a809295d586
Revises: 0eadac2ac523
Create Date: 2018-05-04 19:37:29.033365

"""

from alembic import context
from sqlalchemy.orm import sessionmaker
from limonero.migration_utils import (downgrade_actions, upgrade_actions,
        is_mysql, get_psql_enum_alter_commands, is_psql, is_sqlite)

# revision identifiers, used by Alembic.
revision = '6a809295d586'
down_revision = '0eadac2ac523'
branch_labels = None
depends_on = None


def get_commands():

    values = ['XML_FILE','NETCDF4','HDF5','SHAPEFILE','TEXT','CUSTOM','JSON',
                            'CSV','PICKLE','GEO_JSON']
    if is_mysql():
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
