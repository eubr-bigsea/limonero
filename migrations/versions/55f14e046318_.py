"""empty message

Revision ID: 55f14e046318
Revises: 19fa4bf1351a
Create Date: 2018-08-07 15:23:11.354156

"""
from alembic import op
from sqlalchemy import String, Integer
from sqlalchemy.sql import table, column, text
from sqlalchemy.dialects import mysql
# revision identifiers, used by Alembic.
revision = '55f14e046318'
down_revision = '19fa4bf1351a'
branch_labels = None
depends_on = None

storage_table = table('storage',
                      column("id", Integer),
                      column("name", String),
                      column('type', String),
                      column('url', String),
                      column('enabled', Integer)
                      )


def upgrade():
    op.bulk_insert(storage_table,
                   [
                       {
                           'name': 'Default (local)',
                           'type': 'HDFS',
                           'url': 'file:///srv/storage/',
                           'enabled': 1
                       }
                   ])

def downgrade():
    op.execute(text("DELETE FROM storage WHERE url = 'file:///srv/storage/'"))
