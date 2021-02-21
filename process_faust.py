import faust

# Creating the faust App and assigning the consumer group to consume from Kafka topic
app = faust.App('test1', broker='kafka://localhost:9092', value_serializer='json')


# Output
class Stats(faust.Record):
    device_id: int
    timestamp_start: float
    minm: int
    maxm: int
    avg: float


# Initiate Faust Topic, Faust Table, Dictionaries
topic = app.topic('TestTopic')
table = app.Table('counter', partitions=1, default=list)
processed_time, delayed_devices = dict(), dict()


# Convert timestamp to nearest timestamp min
def convert_timestamp(time):
    time = int(time)
    return time - (time % 60)


# On receiving the incoming stream
@app.agent(topic)
async def receiver(stream):
    last_timestamp = 0
    async for s in stream:
        device_id, value, timestamp = s.split(',')
        device_id = int(device_id[11:])
        value = int(value[7:])
        curr_timestamp = convert_timestamp(int(timestamp[12:]))

        # For delayed timestamps
        if (curr_timestamp - last_timestamp) < 0:
            await handle_delay(device_id, value, curr_timestamp)
            continue

        if (curr_timestamp - last_timestamp) != 0 and (curr_timestamp - last_timestamp) != curr_timestamp:
            await avg_stats(last_timestamp)

        last_timestamp = curr_timestamp
        await eval_stats(device_id, value, last_timestamp)


# Calculating min,max,count,sum for each device in each min
async def eval_stats(id, val, time):
    device_id = id
    value = val
    curr_timestamp = time
    if not table[(device_id, curr_timestamp)]:
        table[(device_id, curr_timestamp)] = [value, value, 1, value]
    else:
        val = table[(device_id, curr_timestamp)]
        minm = min(val[0], value)
        maxm = max(val[1], value)
        count = val[2] + 1
        sum = val[3] + value
        table[(device_id, curr_timestamp)] = [minm, maxm, count, sum]


# Calculating average for each device in each min
async def avg_stats(curr_minute):
    keys = [k for k in table.keys() if k[1] == curr_minute]
    vals = [table[k] for k in table.keys() if k[1] == curr_minute]
    for k, v in zip(keys, vals):
        avg = v[3] / v[2]
        await store_to_db([k[0], curr_minute, v[0], v[1], avg])
    # processed_time['time'] = curr_minute
    # print("Process_time", processed_time['time'])
    # Deleting processed stats from table
    for del_k in keys:
        del table[del_k]


# Pushing the processed stats to db of each device in each min
async def store_to_db(out):
    if 'time' not in processed_time.keys():
        processed_time['time']=out[1]
    else:
        processed_time['time']=out[1]
    print("Store_DB", out)


# To process last timestamp of data stream or Delay in data stream
@app.timer(interval=60.0)
async def per_min():
    delayed_keys = [k for k in delayed_devices.keys()]
    if delayed_keys:
        for k, v in delayed_devices.items():
            await store_to_db([k[0], k[1], v[0], v[1], v[4]])
    for del_k in delayed_keys:
        del delayed_devices[del_k]

    #For last timestamp in Kafka Topic
    keys = [k for k in table.keys() if (k[1] - processed_time['time']) >= 60]
    if keys:
        for k in keys:
            print("per min", [k[1], processed_time['time']])
            await process_last_timestamp(k[1])


async def process_last_timestamp(time):
    keys = [k for k in table.keys() if k[1] == time]
    vals = [table[k] for k in table.keys() if k[1] == time]
    for k, v in zip(keys, vals):
        avg = v[3] / v[2]
        await store_to_db([k[0], time, v[0], v[1], avg])


# Handle delay of 1 minute(60 seconds)
async def handle_delay(id, val, delay):
    if (id, delay) not in delayed_devices.keys():
        delayed_devices[(id, delay)] = [val, val, 1, val, float(val)]
    else:
        data = delayed_devices[(id, delay)]
        minm = min(data[0], val)
        maxm = max(data[1], val)
        count = data[2] + 1
        sum = data[3] + val
        avg = sum / count
        delayed_devices[(id, delay)] = [minm, maxm, count, sum, float(avg)]


if __name__ == '__main__':
    app.main()
