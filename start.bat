@ECHO OFF
echo Starting Zookeeper
START "" .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
timeout 18
echo Starting Kafka Server
START "" .\bin\windows\kafka-server-start.bat .\config\server.properties
timeout 30
echo Creating Topic for the data-stream: Topic
START "" .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic KafkaTopic
timeout 20
PAUSE

