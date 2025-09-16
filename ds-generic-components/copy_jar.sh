set -o xtrace
# JAR_NAME="rdbms-integration"
# JAR_NAME="spark-shape-monitor"
JAR_NAME="spark-cube"
rm -v ~/git/dcd/orchestration-building-blocks/src/orchestration/spark/$JAR_NAME.jar
./gradlew :$JAR_NAME:clean :$JAR_NAME:assemble
cp ./$JAR_NAME/build/libs/$JAR_NAME-*.jar ~/git/dcd/orchestration-building-blocks/src/orchestration/spark/$JAR_NAME.jar
ls -l ~/git/dcd/orchestration-building-blocks/src/orchestration/spark/ | grep .jar
aws s3 rm s3://datacloud-datascience-code-store-708035784431tf/tmp/spark-job-dxrtest/$JAR_NAME.jar
aws s3 cp ./$JAR_NAME/build/libs/$JAR_NAME-*.jar s3://datacloud-datascience-code-store-708035784431tf/tmp/spark-job-dxrtest/$JAR_NAME.jar
set +o xtrace