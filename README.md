# Data-Stream-Analytics Service

Any operating system would be sufficient for this application. The service has been built on Windows 10 operating system.

Following are the steps to run the application on Windows:

1. Install Zookeeper and Apache Kafka Server
Go to https://kafka.apache.org/downloads and download one of the binary downloads. Extract the downloaded zip file into the directory of your choice.
For example C:/Kafka

2. In extracted Kafka folder, go to the windows directory and modify value of 'log.dirs' in server.properties to the directory where Kafka folder is present i.e. C:/kafka/kafka-logs
Similary modify the 'dataDir' in zookeeper.properties to the directory where Kafka folder is present i.e. C:/kafka/zookeeper-data

3. Install Faust by running pip install -U faust in command prompt.
https://faust.readthedocs.io/en/latest/userguide/installation.html

4. Install Mongo DB and Mongo DB drivers
   (a) Run pip install pymongo in command prompt to get pymongo Python MongoDB driver
   (b) Run python -m pip install motor in command prompt to get Motor MongoDB driver (https://motor.readthedocs.io/en/stable/)
   (c) Go to https://www.mongodb.com/try/download/community and download the current MongoDB community server. Keep the default settings and install.
   (d) Add C:\Program Files\MongoDB\Server\3.4\bin into environment variables, so that we can run mongod.exe from anywhere in the system.

5. Add the data into input_data.txt file in the format of device_id: 1, value: 1, timestamp: 1611741600. There is a test .txt file given for reference.

6. Open a command prompt and run mongod to start MongoDB

7. Run the start.bat file. It will start the Zookeeper and Kafka Server. It will also create a Kafka Topic.

8. Run main.py to initiate the Kafka Producer to produce data to the topic.You will see the data being sent to Topic with Topic Name and Partition as Output.

8. Open a command prompt, make sure that you are in the same directory as process_faust.py. Enter faust -A process_faust worker -l info into the command prompt. This will start a worker agent for the Faust App.
You will see entries being processed as output on command prompt. Wait for few minutes as we have some timer functions executing as well. When you see 'Last_timestamp processed', it means we have reached the end of data stream.

9. Once the processing is done, in a new command window you can execute Db_data.py to see the data stored in MongoDB

10. It is suggested to install everything in the same directory to run these commands with ease. If you are changing the directories, make sure to either modify the start.bat accordingly or run the Zookeeper and Kafka server individually.

11. To test this application, you can use already given input_data.txt file. Please make sure to keep the input_data.txt in the same folder and also not to change the filename as this is the path present in the code.
