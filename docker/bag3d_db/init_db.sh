#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE bag3d_tester WITH LOGIN PASSWORD 'bag3d_test';
  CREATE DATABASE bag3d_db WITH OWNER bag3d_tester;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname bag3d_db <<-EOSQL
  CREATE EXTENSION postgis;
EOSQL

pg_restore \
--username "$POSTGRES_USER" \
--dbname bag3d_db \
/tmp/bag3d_db.dump
