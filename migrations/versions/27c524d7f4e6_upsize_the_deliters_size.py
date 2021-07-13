"""Upsize the deliters size

Revision ID: 27c524d7f4e6
Revises: 0d09368b6717
Create Date: 2017-09-06 14:17:02.366800

"""
import sqlalchemy as sa
from alembic import op
from limonero.migration_utils import is_sqlite

# revision identifiers, used by Alembic.
revision = '27c524d7f4e6'
down_revision = '0d09368b6717'
branch_labels = None
depends_on = None


def upgrade():
    if is_sqlite():
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.alter_column('attribute_delimiter',
                        existing_type=sa.String(length=4),
                        type_=sa.VARCHAR(length=20),
                        existing_nullable=True)
            batch_op.alter_column('record_delimiter',
                            existing_type=sa.String(length=4),
                            type_=sa.VARCHAR(length=20),
                            existing_nullable=True)
            batch_op.alter_column('text_delimiter',
                            existing_type=sa.String(length=4),
                            type_=sa.VARCHAR(length=20),
                            existing_nullable=True)
            batch_op.add_column(sa.Column(
                'encoding', sa.String(length=20), nullable=True))
        
            batch_op.add_column(sa.Column('is_first_line_header', sa.Boolean(),
                                    nullable=False, server_default='0'))

    else:
        op.alter_column('data_source', 'attribute_delimiter',
                        existing_type=sa.String(length=4),
                        type_=sa.VARCHAR(length=20),
                        existing_nullable=True)
        op.alter_column('data_source', 'record_delimiter',
                        existing_type=sa.String(length=4),
                        type_=sa.VARCHAR(length=20),
                        existing_nullable=True)
        op.alter_column('data_source', 'text_delimiter',
                        existing_type=sa.String(length=4),
                        type_=sa.VARCHAR(length=20),
                        existing_nullable=True)
        op.add_column('data_source', sa.Column(
            'encoding', sa.String(length=20), nullable=True))
    
        op.add_column('data_source',
                      sa.Column('is_first_line_header', sa.Boolean(),
                                nullable=False, server_default='0'))


def downgrade():
    if is_sqlite():
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.alter_column('attribute_delimiter',
                        existing_type=sa.String(length=20),
                        type_=sa.VARCHAR(length=4),
                        existing_nullable=True)
            batch_op.alter_column('record_delimiter',
                            existing_type=sa.String(length=20),
                            type_=sa.VARCHAR(length=4),
                            existing_nullable=True)
            batch_op.alter_column('text_delimiter',
                            existing_type=sa.String(length=20),
                            type_=sa.VARCHAR(length=4),
                            existing_nullable=True)
            batch_op.drop_column('encoding')
            batch_op.drop_column('is_first_line_header')
    else:
        op.alter_column('data_source', 'attribute_delimiter',
                        existing_type=sa.String(length=20),
                        type_=sa.VARCHAR(length=4),
                        existing_nullable=True)
        op.alter_column('data_source', 'record_delimiter',
                        existing_type=sa.String(length=20),
                        type_=sa.VARCHAR(length=4),
                        existing_nullable=True)
        op.alter_column('data_source', 'text_delimiter',
                        existing_type=sa.String(length=20),
                        type_=sa.VARCHAR(length=4),
                        existing_nullable=True)
        op.drop_column('data_source', 'encoding')
        op.drop_column('data_source', 'is_first_line_header')
