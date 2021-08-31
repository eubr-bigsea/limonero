"""empty message

Revision ID: 2ce4ce388a0e
Revises: 1f797e7ccce7
Create Date: 2017-06-12 17:56:20.157142

"""
from alembic import op
import sqlalchemy as sa
from limonero.migration_utils import is_sqlite


# revision identifiers, used by Alembic.
revision = '2ce4ce388a0e'
down_revision = '1f797e7ccce7'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('attribute', sa.Column('scale', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    if is_sqlite():
        with op.batch_alter_table('attribute') as batch_op:
            batch_op.drop_column('scale')
    else:
        op.drop_column('attribute', 'scale')
    # ### end Alembic commands ###
