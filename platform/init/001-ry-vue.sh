#!/bin/bash
set -e

# The upstream SQL contains Chinese seed data. Force the init client to use
# utf8mb4 so MySQL does not interpret UTF-8 bytes as latin1 characters.
mysql --protocol=socket \
  --default-character-set=utf8mb4 \
  -uroot -p"${MYSQL_ROOT_PASSWORD}" \
  "${MYSQL_DATABASE}" < /docker-entrypoint-initdb.d/ry_vue.seed
