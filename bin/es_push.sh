#!/bin/bash

DIR_HOME=$(cd $(dirname $0); pwd)

CLASSPATH="/home/es/elasticsearch-2.0.0/lib/*:$DIR_HOME/lib/*:/opt/cloudera/parcels/CDH/jars/*"
CLASS="com.cobub.es.dao.PushData"
JAVA_OPTS="-Xmn256m -Xmx2048m"
LOG_PATH="$DIR_HOME/logs"
RUN_PATH="$DIR_HOME/run"
LOG_FILE="es-tag-test.log"
PID_FILE="es-tag-test.pid"

if [ ! -d $LOG_PATH ];then
  mkdir -p $LOG_PATH
fi

if [ ! -d $RUN_PATH ];then
  mkdir -p $RUN_PATH
fi 

run () {
  if [ -f $RUN_PATH/$PID_FILE ]; then
    echo "$RUN_PATH/$PID_FILE already exists."
    echo "Now exiting ..."
    exit 1
  fi
  
  $@ > $LOG_PATH/$LOG_FILE 2>&1 &
  PID=$!
  echo "pid=$PID"
  echo $PID > "$RUN_PATH/$PID_FILE"
  wait $PID
  rm -f $RUN_PATH/$PID_FILE
}

CMD="$JAVA_HOME/bin/java -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8 -cp $CLASSPATH $JAVA_OPTS $CLASS"
echo $CMD
run $CMD
