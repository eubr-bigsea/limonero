"""adding entity Model

Revision ID: a1c5e55317e4
Revises: 2ce4ce388a0e
Create Date: 2017-06-19 13:09:16.599850

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'a1c5e55317e4'
down_revision = '2ce4ce388a0e'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('model',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('name', sa.String(length=100), nullable=False),
                    sa.Column('enabled', sa.Boolean(), nullable=False),
                    sa.Column('path', sa.String(length=500), nullable=False),
                    sa.Column('type',
                              sa.Enum('SPARK_ML_REGRESSION', 'UNSPECIFIED',
                                      'SPARK_MLLIB_CLASSIFICATION',
                                      'SPARK_ML_CLASSIFICATION',
                                      name='ModelTypeEnumType'),
                              nullable=False),
                    sa.Column('user_id', sa.Integer(), nullable=False),
                    sa.Column('user_login', sa.String(length=50),
                              nullable=False),
                    sa.Column('user_name', sa.String(length=200),
                              nullable=False),
                    sa.Column('storage_id', sa.Integer(), nullable=False),
                    sa.ForeignKeyConstraint(['storage_id'], ['storage.id'], ),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_table('model_permission',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('permission',
                              sa.Enum('READ', 'MANAGE', 'WRITE',
                                      name='PermissionTypeEnumType'),
                              nullable=False),
                    sa.Column('user_id', sa.Integer(), nullable=False),
                    sa.Column('user_login', sa.String(length=50),
                              nullable=False),
                    sa.Column('user_name', sa.String(length=200),
                              nullable=False),
                    sa.Column('model_id', sa.Integer(), nullable=False),
                    sa.ForeignKeyConstraint(['model_id'], ['model.id'], ),
                    sa.PrimaryKeyConstraint('id')
                    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('model_permission')
    op.drop_table('model')
    # ### end Alembic commands ###
