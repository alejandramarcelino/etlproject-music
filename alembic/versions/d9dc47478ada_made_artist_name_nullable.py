"""made artist name nullable

Revision ID: d9dc47478ada
Revises: 0b79136e1e36
Create Date: 2023-05-29 19:17:16.345706

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd9dc47478ada'
down_revision = '0b79136e1e36'
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
