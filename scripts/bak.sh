#!/bin/bash
docker exec -i my-mysql-container sh -c 'mysql -uroot -p${MYSQL_ROOT_PASSWORD} -e "CREATE DATABASE IF NOT EXISTS csbot_bak CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"'
docker exec -i my-mysql-container sh -c 'mysqldump -uroot -p${MYSQL_ROOT_PASSWORD} csbot | mysql -uroot -p${MYSQL_ROOT_PASSWORD} csbot_bak'