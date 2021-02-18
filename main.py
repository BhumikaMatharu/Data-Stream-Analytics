
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from kafka import KafkaProducer,KafkaConsumer
import json
import sys


def kafka_producer(input_file,server,topic):

    # Creating Kafka Producer to listen to data stream coming from devices
    producer = KafkaProducer(bootstrap_servers=server,
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))


    with open(input_file,'r') as file:
        for line in file.readlines():
            ack = producer.send(topic, line)
            metadata = ack.get()
            print(msg)
            print(metadata.topic)
            print(metadata.partition)

def kafka_consumer(server,topic):
    consumer = KafkaConsumer(topic, group_id='1',
                             bootstrap_servers=server,
                             auto_offset_reset='earliest')
    # when consumer fails, it goes for earliest or latest

    #     for message in consumer:
    #         print(message.topic,"value:",message.value)
    # except Exception as ex:
    #     print("Error", ex)
    try:
        for message in consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))

    except KeyboardInterrupt:
        sys.exit()

    finally:
        consumer.close()
    # print(data_stream)
# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    bootstrap_servers = ['localhost:9092']
    topic_name = 'TestTopic'
    kafka_producer('Input_data.txt',bootstrap_servers,topic_name)
    # kafka_consumer(bootstrap_servers,topic_name)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
