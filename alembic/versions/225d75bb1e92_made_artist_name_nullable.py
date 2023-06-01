"""made artist name nullable

Revision ID: 225d75bb1e92
Revises: d9dc47478ada
Create Date: 2023-05-29 19:19:22.487778

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '225d75bb1e92'
down_revision = 'd9dc47478ada'
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
