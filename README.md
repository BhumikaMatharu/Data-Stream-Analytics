# Data-Stream-Analytics Service

Any operating system would be sufficient for this application. The service has been built on Windows 10 operating system.

Following are the steps to run the application on Windows:

1. Install Zookeeper and Apache Kafka Server
Go to https://kafka.apache.org/downloads and download one of the binary downloads. Extract the downloaded zip file into the directory of your choice.
For example C:/Kafka

2. In extracted Kafka folder, go to the windows directory and modify value of 'log.dirs' in server.properties to the directory where Kafka folder is present i.e. C:/kafka/kafka-logs
Similary modify the 'dataDir' in zookeeper.properties to the directory where Kafka folder is present i.e. C:/kafka/zookeeper-data

3. Install Faust by running pip install -U faust in command prompt. pip install -U faust.
https://faust.readthedocs.io/en/latest/userguide/installation.html

4.Install Mongo DB and Mongo DB drivers
(a)Run pip install pymongo in command prompt to get pymongo Python MongoDB driver
(b)Run python -m pip install motor in command prompt to get Motor MongoDB driver (https://motor.readthedocs.io/en/stable/)
(c)Go to https://www.mongodb.com/try/download/community and download the current MongoDB community server. Keep the default settings and install.
(d)Add C:\Program Files\MongoDB\Server\3.4\bin into environment variables, so that we can run mongod.exe from anywhere in the system.

5. Open a command prompt and run mongod to start MongoDB

4. Run the start.bat file. It will start the Zookeeper and Kafka Server. It will also create a Kafka Topic and run main.py to initiate the Kafka Producer to produce data to the topic.

5. Open a command prompt, make sure that you are in the same directory as process_faust.py. Enter faust -A process_faust worker -l info into the command prompt. This will start a worker agent for the Faust App.

6. Once the processing is done, you can execute Db_data.py to get the data stored in MongoDB

