import faust
from py_mongodb import MongoClient

# Creating the faust App and assigning the consumer group to consume from Kafka topic
app = faust.App('cons_grp', broker='kafka://localhost:9092', value_serializer='json')

# Initiate Faust Topic, Faust Table, Dictionaries
topic = app.topic('KafkaTopic')
table = app.Table('counter', partitions=1, default=list)
processed_timestamp = {'time': 0}
delayed_devices = dict()


# Convert timestamp to nearest timestamp min
def convert_timestamp(time):
    time = int(time)
    return time - (time % 60)


# Output Format
def create_document(device: int, time: float, minm: int, maxm: int, avg: float):
    js = {
        'device_id': device,
        'timestamp_start': time,
        'min': minm,
        'max': maxm,
        'avg': avg
    }
    return js


# On receiving the incoming stream
@app.agent(topic)
async def receiver(stream):
    last_timestamp = 0
    async for s in stream:
        device_id, value, timestamp = s.split(',')
        device_id = int(device_id.split(':')[-1].strip())
        value = int(value.split(':')[-1].strip())
        curr_timestamp = int(convert_timestamp(timestamp.split(':')[-1].strip()))
        # For delayed timestamps
        if (curr_timestamp - last_timestamp) < 0:
            await handle_delay(device_id, value, curr_timestamp)
            continue

        if (curr_timestamp - last_timestamp) != 0 and (curr_timestamp - last_timestamp) != curr_timestamp:
            await avg_stats(last_timestamp)

        last_timestamp = curr_timestamp
        await eval_stats(device_id, value, last_timestamp)


# Calculating min,max,count,sum for each device in each min
async def eval_stats(id, value, time):
    if not table[(id, time)]:
        table[(id, time)] = [value, value, 1, value]
    else:
        val = table[(id, time)]
        minm = min(val[0], value)
        maxm = max(val[1], value)
        count = val[2] + 1
        sum = val[3] + value
        table[(id, time)] = [minm, maxm, count, sum]


# Calculating average for each device in each min
async def avg_stats(curr_minute):
    keys = [k for k in table.keys() if k[1] == curr_minute]
    vals = [table[k] for k in table.keys() if k[1] == curr_minute]
    records = []
    for k, v in zip(keys, vals):
        avg = v[3] / v[2]
        records.append(create_document(k[0], curr_minute, v[0], v[1], avg))
    await store_to_db(records, 0)


# Pushing the processed stats to db of each device in each min
async def store_to_db(out, flag):
    print("Hit Store to DB",[out,flag])
    mongo = MongoClient()
    if not flag:
        if len(out) == 1:
            await mongo.insert_one(out[0])
            processed_timestamp['time'] = out[0]['timestamp_start']
        else:
            await mongo.insert_many(out)
            processed_timestamp['time'] = out[-1]['timestamp_start']
    else:
        for o in out:
            query = {'device_id': o['device_id'], 'timestamp_start': o['timestamp_start']}
            record = await mongo.find_document(query)
            key = (o['device_id'], o['timestamp_start'])
            if record:
                minm = min(o['min'], record['min'])
                maxm = max(o['max'], record['max'])
                avg = (table[key][3] + delayed_devices[key][3]) / (table[key][2] + delayed_devices[key][2])
                new_record = create_document(key[0], key[1], minm, maxm, avg)
                await mongo.update_document(query, new_record)
            else:
                avg = delayed_devices[key][3] / delayed_devices[key][2]
                record = create_document(key[0], key[1], delayed_devices[key][0], delayed_devices[key][1], avg)
                await mongo.insert_one(record)
            del delayed_devices[key]


# To process last timestamp of data stream or Delay in data stream
@app.timer(interval=60.0)
async def per_min():
    delayed_keys = [k for k in delayed_devices.keys()]
    records = []
    if delayed_keys:
        for k, v in delayed_devices.items():
            avg = v[3] / v[2]
            records.append(create_document(k[0], k[1], v[0], v[1], avg))
        await store_to_db(records, 1)

    # For last timestamp in Kafka Topic
    keys = [k for k in table.keys() if (k[1] - processed_timestamp['time']) >= 60]
    if keys:
        for k in keys:
            await avg_stats(k[1])
        print("Last_timestamp processed")


# Handle delay of 1 minute(60 seconds)
async def handle_delay(id, val, delay):
    if (id, delay) not in delayed_devices.keys():
        delayed_devices[(id, delay)] = [val, val, 1, val]
    else:
        data = delayed_devices[(id, delay)]
        minm = min(data[0], val)
        maxm = max(data[1], val)
        count = data[2] + 1
        sum = data[3] + val
        delayed_devices[(id, delay)] = [minm, maxm, count, sum]


if __name__ == '__main__':
    app.main()
