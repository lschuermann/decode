"""Create the initial database layout

Revision ID: d9e216be35fd
Revises: 
Create Date: 2022-10-31 17:45:00.858075

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'd9e216be35fd'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('objects',
        sa.Column('id', postgresql.UUID(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('objects')
