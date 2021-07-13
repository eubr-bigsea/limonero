"""empty messagepark-2.3.0/

Revision ID: 275b0e49dff7
Revises: 66d4be40bced
Create Date: 2018-07-11 16:15:33.196417

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql
from sqlalchemy.sql import text
from limonero.migration_utils import (downgrade_actions, upgrade_actions,
        is_mysql, is_psql, is_sqlite, get_psql_enum_alter_commands)

# revision identifiers, used by Alembic.
revision = '275b0e49dff7'
down_revision = '66d4be40bced'
branch_labels = None
depends_on = None

def get_commands():
    values = ['HDFS', 'OPHIDIA','ELASTIC_SEARCH','MONGODB',
                 'POSTGIS','HBASE','CASSANDRA','JDBC']
    return [
            [
               get_psql_enum_alter_commands(['storage'], ['type'], 
                   'StorageTypeEnumType', values, 'HDFS'),
               get_psql_enum_alter_commands(['storage'], ['type'], 
                   'StorageTypeEnumType', values, 'HDFS'),
             ]
    ]

def upgrade():

    if is_mysql():
        op.add_column('data_source',
                  sa.Column('command', mysql.LONGTEXT(), nullable=True))
        op.get_bind().execute(text("""
            ALTER TABLE storage CHANGE `type` `type` 
            ENUM('HDFS', 'OPHIDIA','ELASTIC_SEARCH','MONGODB',
                 'POSTGIS','HBASE','CASSANDRA','JDBC') CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;"""
                                   ))
    elif is_psql():
        op.add_column('data_source',
                  sa.Column('command', sa.Text(), nullable=True))
        upgrade_actions(get_commands())

    if is_sqlite():
        with op.batch_alter_table('storage') as batch_op:
            batch_op.add_column(sa.Column('enabled', sa.Boolean(), nullable=False, server_default='true'))
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.add_column(sa.Column('updated', sa.DateTime(), nullable=False, server_default='2021-01-01'))
            batch_op.add_column(sa.Column('command', sa.Text(), nullable=True))
    else:
        op.add_column('storage', sa.Column('enabled', sa.Boolean(), nullable=False,
                                       server_default=sa.schema.DefaultClause(
                                           "1"), default=1))
        op.add_column('data_source',
                  sa.Column('updated', sa.DateTime(), nullable=False,
                            server_default='2018-01-01'))


def downgrade():
    if is_mysql():
        op.get_bind().execute(text("""
                ALTER TABLE storage CHANGE `type` `type` 
                ENUM('HDFS', 'OPHIDIA','ELASTIC_SEARCH','MONGODB',
                     'POSTGIS','HBASE','CASSANDRA') CHARSET utf8 COLLATE utf8_unicode_ci NOT NULL;"""
                                       ))
    elif is_psql():
        downgrade_actions(get_commands())
    if is_sqlite():
        with op.batch_alter_table('storage') as batch_op:
            batch_op.drop_column('enabled')
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.drop_column('updated')
            batch_op.drop_column('command')
    else:
        op.drop_column('data_source', 'command')
        op.drop_column('storage', 'enabled')
        op.drop_column('data_source', 'updated')
