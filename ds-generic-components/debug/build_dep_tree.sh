# ./gradlew allDeps > dep.log

set xtrace -o
set -e

project_root=$(echo `pwd`| sed -e "s/orchestration-deployer.*/orchestration-deployer/g")

$project_root/gradlew :rdbms-integration:dependencies > $project_root/debug/dep_rdbms-integration.log
$project_root/gradlew :spark-adp-commons:dependencies > $project_root/debug/dep_spark-adp-commons.log
$project_root/gradlew :spark-connect-by:dependencies > $project_root/debug/dep_spark-connect-by.log
$project_root/gradlew :spark-cube:dependencies > $project_root/debug/dep_spark-cube.log
$project_root/gradlew :spark-data-sync:dependencies > $project_root/debug/dep_spark-data-sync.log
$project_root/gradlew :spark-feature-engineering:dependencies > $project_root/debug/dep_spark-feature-engineering.log
$project_root/gradlew :spark-insight-engine:dependencies > $project_root/debug/dep_spark-insight-engine.log
$project_root/gradlew :spark-shape-monitor:dependencies > $project_root/debug/dep_spark-shape-monitor.log
$project_root/gradlew :spark-sql-wrapper:dependencies > $project_root/debug/dep_spark-sql-wrapper.log

# cat dep_rdbms-integration.log >> dep.log
# cat dep_spark-adp-commons.log >> dep.log
# cat dep_spark-connect-by.log >> dep.log
# cat dep_spark-cube.log >> dep.log
# cat dep_spark-data-sync.log >> dep.log
# cat dep_spark-feature-engineering.log >> dep.log
# cat dep_spark-insight-engine.log >> dep.log
# cat dep_spark-shape-monitor.log >> dep.log
# cat dep_spark-sql-wrapper.log >> dep.log

#  ./gradlew testClasses

set xtrace +o