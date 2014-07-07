set -e
cd /Users/xiaohu/program/insight/project/hashtags/
mvn clean package
cd /tmp
storm dev-zookeeper &
storm nimbus &
storm supervisor &
storm ui &
storm drpc &
sleep 10
storm jar /Users/xiaohu/program/insight/project/hashtags/target/hashtags-1.0-SNAPSHOT-jar-with-dependencies.jar hashtags.HashtagTopology