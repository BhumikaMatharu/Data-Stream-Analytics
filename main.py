from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# Validating input data format
def validate_input(input):
    device_id, value, timestamp = input.split(',')
    device_id = device_id[11:]
    value = value[7:]
    curr_timestamp = timestamp[12:]
    if device_id.isdigit() and value.isdigit() and curr_timestamp.isdigit():
        return True
    else:
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
                    producer.send(topic, line)
                except KafkaError as err:
                    print("Something went wrong, Please check logs")
                    raise err
            else:
                print("Invalidate Input Data")
                raise Exception


if __name__ == '__main__':
    topic = 'Topic'
    bootstrap_servers = ['localhost:9092']
    kafka_producer('input_data.txt', bootstrap_servers, topic)
