from kafka import KafkaProducer
import json


def kafka_producer(input_file,server,topic):

    # Creating Kafka Producer to listen to data stream coming from devices
    producer = KafkaProducer(bootstrap_servers=server,
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))


    with open(input_file,'r') as file:
        for line in file.readlines():
            ack = producer.send(topic, line)
            metadata = ack.get()
            print(line)
            print(metadata.topic)
            print(metadata.partition)

if __name__ == '__main__':

    topic = 'TestTopic2'
    bootstrap_servers = ['localhost:9092']
    kafka_producer('Input_data.txt',bootstrap_servers,topic)

