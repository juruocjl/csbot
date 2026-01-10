#!/bin/bash
docker exec -it csbot-database psql -U postgres -c "CREATE DATABASE csbot_backup;"
docker exec csbot-database pg_dump -U postgres csbot | docker exec -i csbot-database psql -U postgres csbot_backup