#!/usr/bin/env bash
SRC_HOME="$(cd "`dirname "$0"`"/; pwd)"

prefix=$SRC_HOME/../src/main/python/fdata

depend_files=$SRC_HOME/../package/pysigma.zip

echo 'prepare stock_unclose_prd_long_data t-2 data'

spark2-submit --master yarn --py-files $depend_files --executor-memory 2G  --num-executors 4 ${prefix}/stock_profit/prepare_close_prd.py
spark2-submit --master yarn --py-files $depend_files --executor-memory 2G  --num-executors 4 ${prefix}/stock_profit/prepare_unclose_prd.py
