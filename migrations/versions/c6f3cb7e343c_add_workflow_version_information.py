"""Add workflow version information

Revision ID: c6f3cb7e343c
Revises: c74c284e47e6
Create Date: 2023-07-10 17:38:00.354511

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'c6f3cb7e343c'
down_revision = 'c74c284e47e6'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('data_source', sa.Column('workflow_version', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_data_source_task_id'), 'data_source', ['task_id'], unique=False)
    op.create_index(op.f('ix_data_source_workflow_id'), 'data_source', ['workflow_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_data_source_workflow_id'), table_name='data_source')
    op.drop_index(op.f('ix_data_source_task_id'), table_name='data_source')
    op.drop_column('data_source', 'workflow_version')
    # ### end Alembic commands ###
