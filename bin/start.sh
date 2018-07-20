#!bin/bash

SRC_HOME="$(cd "`dirname "$0"`"/; pwd)"

prefix=$SRC_HOME/../src/main/python/fdata

depend_files=$SRC_HOME/../package/pysigma.zip

spark2-submit --master yarn \
    --py-files $depend_files \
    --executor-memory 2G  \
    --num-executors 4  \
    ${prefix}/runjob.py \
    -f merge -s 2017-03-17 -e 2017-04-15

spark2-submit --master yarn \
    --py-files $depend_files \
    --executor-memory 2G  \
    --num-executors 4  \
    ${prefix}/runjob.py \
    -f check -s 2017-03-17 -e 2017-04-15


spark2-submit --master yarn \
    --py-files $depend_files \
    --executor-memory 2G  \
    --num-executors 4  \
    ${prefix}/runjob.py \
    -f check_exception_detail -s 2017-03-17 -e 2017-04-15


spark2-submit --master yarn \
    --py-files $depend_files \
    --executor-memory 2G  \
    --num-executors 4  \
    ${prefix}/runjob.py \
    -f check_exception_stat -s 2017-03-17 -e 2017-04-15
