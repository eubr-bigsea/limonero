"""Data source initialization

Revision ID: 422f46c29791
Revises: 96581239a707
Create Date: 2019-10-07 20:45:35.783397

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from limonero.migration_utils import is_mysql, is_psql, is_sqlite

# revision identifiers, used by Alembic.
revision = '422f46c29791'
down_revision = '96581239a707'
branch_labels = None
depends_on = None


def upgrade():
    if is_psql():
        ds_enum = postgresql.ENUM('NO_INITIALIZED', 'INITIALIZING', 
            'INITIALIZED', name='DataSourceInitializationEnumType')
        ds_enum.create(op.get_bind())

    if is_sqlite():
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.add_column(sa.Column('initialization', sa.String(length=100), nullable=False,
                server_default='NO_INITIALIZED'))
    else:
        op.add_column('data_source', sa.Column(
            'initialization', sa.Enum('NO_INITIALIZED', 'INITIALIZING', 
                'INITIALIZED', name='DataSourceInitializationEnumType'), nullable=False))

def downgrade():
    if is_sqlite():
        with op.batch_alter_table('data_source') as batch_op:
            batch_op.drop_column('initialization')
    else:
        op.drop_column('data_source', 'initialization')
    if is_psql():
        op.get_bind().execute('DROP TYPE "DataSourceInitializationEnumType"')

