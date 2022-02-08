#!/bin/bash

## How to run
##   `./upgrade.sh from to`
##
## such as:
##   `./upgrade.sh 0.3.1 0.3.2`

if [ "$#" -ne 2 ]
then
   echo "Arguments are not equals to 2\n"
   echo "./upgrade.sh from to"
   exit 1
fi

sed -i '' "s/$1/$2/g" Cargo.toml libduckdb-sys/upgrade.sh libduckdb-sys/Cargo.toml .github/workflows/rust.yaml
./libduckdb-sys/upgrade.sh
