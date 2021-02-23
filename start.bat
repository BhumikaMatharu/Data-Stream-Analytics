@ECHO OFF
echo Starting Zookeeper
START "" .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
timeout 18
echo Starting Kafka Server
START "" .\bin\windows\kafka-server-start.bat .\config\server.properties
timeout 30
echo Creating Topic for the data-stream: CiscoMeraki
START "" .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic CiscoMeraki
timeout 20
echo Starting Kafka Producer
START "" python main.py
PAUSE

