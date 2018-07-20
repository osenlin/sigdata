#!/usr/bin/env bash

SRC_HOME="$(cd "`dirname "$0"`"/; pwd)"

PACKAGE_NAME=pysigma.zip

ZIP_FILE=$SRC_HOME/../src/main/python/

mkdir -p $SRC_HOME/../package

PACKAGE_PATH=$SRC_HOME/../package/$PACKAGE_NAME

rm -rf $PACKAGE_PATH

cd $ZIP_FILE

zip -r $PACKAGE_PATH ./
