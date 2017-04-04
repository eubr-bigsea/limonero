""" Inserts sample storage

Revision ID: efa53423306c
Revises: c1124305c79e
Create Date: 2017-02-16 23:45:00.651862

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column

# revision identifiers, used by Alembic.
revision = 'efa53423306c'
down_revision = 'c1124305c79e'
branch_labels = None
depends_on = None

storage = table('storage',
                column('id', sa.Integer),
                column('name', sa.String),
                column('type', sa.String),
                column('url', sa.String))

def upgrade():
    data = [
        {
            'name': 'HDFS Test storage', 'type': 'HDFS', 
            'url': 'http://spark01:8000'
        }
    ]

    op.bulk_insert(storage, data)


def downgrade():
    bind = op.get_bind()
    for row in bind.execute(storage.select()):
        op.execute(storage.delete().where(storage.c.id == row.id))
