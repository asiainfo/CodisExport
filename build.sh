#!/usr/bin/env bash

HOME_PATH=$(cd `dirname $0`; pwd)

cd ${HOME_PATH}

mvn clean package

if [ $? -ne 0 ]; then
   echo "Build failed..."
   exit 1
fi

rm -fr build

mkdir -p build/Codis_Export/logs

cp -r bin build/Codis_Export
cp -r conf build/Codis_Export

cp -r target/runtimelib build/Codis_Export/lib

cp target/codis-export-data-*.jar build/Codis_Export/lib

cd build

tar czf Codis_Export.tar.gz Codis_Export

exit 0
