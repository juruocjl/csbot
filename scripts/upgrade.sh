#!/bin/bash
echo '==revision=='
uv run alembic revision --autogenerate
echo '==upgrade=='
uv run alembic upgrade head