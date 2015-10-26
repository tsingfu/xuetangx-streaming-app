FWDIR=$(cd `dirname $0`/..;pwd)
cd $FWDIR


# dependent jar
for libjar in `ls $FWDIR/lib/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
  if [ "$ADDJARS" = "" ]; then
    ADDJARS=$libjar
  else
    ADDJARS=$ADDJARS,$libjar
  fi
done

echo CLASSPATH=$CLASSPATH
SPARK_CLASSPATH=$CLASSPATH

export CLASSPATH
export SPARK_CLASSPATH
export ADDJARS

SPARK_HOME=/opt/spark-1.4.1-bin-hadoop2.6

#--total-executor-cores 10 \
#--jars $FWDIR/lib/streaming-core-0.1.0-SNAPSHOT.jar \
nohup sh $SPARK_HOME/bin/spark-submit2 \
--jars $ADDJARS \
--files $FWDIR/conf/streaming-app-test.xml \
--class com.xuetangx.streaming.StreamingApp \
--driver-cores 1 \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 5g \
--verbose \
$FWDIR/lib/streaming-core-0.1.0-SNAPSHOT.jar \
yarn-client XTX-straming $FWDIR/conf/streaming-app-test.xml 1 \
 2>&1 > $FWDIR/logs/`basename $0`.log &

tail -30f $FWDIR/logs/`basename $0`.log