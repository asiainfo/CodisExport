#!/usr/bin/env bash

BIN_PATH=$(cd `dirname $0`; pwd)

for jarFile in `ls $BIN_PATH/../lib/*jar`
do
  CLASSPATH=$CLASSPATH:$jarFile
done

java -cp $CLASSPATH com.asiainfo.codis.ExportData

if [ $? -ne 0 ]; then
   echo "Export data failed!"
   exit 1
fi

exit 0