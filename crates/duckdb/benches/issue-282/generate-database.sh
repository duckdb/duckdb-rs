#!/bin/bash

# SQLite
sqlite3 data/db.sqlite <<EOF
CREATE TABLE IF NOT EXISTS income (
    id INTEGER,
    created_at INTEGER,
    amount REAL,
    category_id INTEGER,
    wallet_id INTEGER,
    meta TEXT
);
.mode csv
.import output.csv income
EOF

# DuckDB
echo "CREATE TABLE income (id INTEGER, created_at INTEGER, amount REAL, category_id INTEGER, wallet_id INTEGER, meta TEXT);" | duckdb data/db.duckdb
echo "COPY income FROM 'output.csv' (HEADER);" | duckdb data/db.duckdb