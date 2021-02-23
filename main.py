from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# Validating input data format
def validate_input(input):
    device_id, value, timestamp = input.split(',')

    device_id = device_id.split(':')[-1].strip()
    value = value.split(':')[-1].strip()
    curr_timestamp = timestamp.split(':')[-1].strip()
    if device_id.isdigit() and value.isdigit() and curr_timestamp.isdigit():
        return True
    else:
        # print(device_id,value,curr_timestamp)
        return False


def kafka_producer(input_file, server, topic):
    # Creating Kafka Producer to listen to data stream coming from devices
    producer = KafkaProducer(bootstrap_servers=server,
                             value_serializer=lambda m: json.dumps(m).encode('ascii'),
                             retries=3)

    with open(input_file, 'r') as file:
        # Synchronous Send
        for line in file.readlines():
            if validate_input(line):
                try:
                    ack = producer.send(topic, line)
                    metadata = ack.get()
                    print(line)
                    print(metadata.topic)
                    print(metadata.partition)
                except KafkaError as err:
                    print("Something went wrong, Please check logs")
                    raise err
            else:
                print("Invalidate Input Data")
                raise Exception


if __name__ == '__main__':
    topic = 'KafkaTopic'
    bootstrap_servers = ['localhost:9092']
    kafka_producer('input_data.txt', bootstrap_servers, topic)
