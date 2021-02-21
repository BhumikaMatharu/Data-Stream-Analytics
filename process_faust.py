import faust

# Creating the faust App and the consumer group
app = faust.App('cisco1', broker='kafka://localhost:9092', value_serializer='json')

# Input
class Stream(faust.Record):
    device_id: int
    value: int
    timestamp: int

# Output
class Stats(faust.Record):
    device_id: int
    timestamp_start: float
    minm: int
    maxm: int
    avg: float


# Topic and Table
topic = app.topic('TestTopic2')
table = app.Table('counter', partitions=1, default=list)


# Convert timestamp to nearest timestamp min
def convert_timestamp(time):
    time = int(time)
    return time - (time % 60)

# On receiving the incoming stream
@app.agent(topic)
async def receive(stream):
    last_timestamp = 0
    async for s in stream:
        device_id, value, timestamp = s.split(',')
        device_id = int(device_id[11:])
        value = int(value[7:])
        curr_timestamp = convert_timestamp(int(timestamp[12:]))

        if (curr_timestamp-last_timestamp)==curr_timestamp:
            last_timestamp = curr_timestamp
            await eval_stats(device_id,value,last_timestamp)
        elif (curr_timestamp-last_timestamp)!=0:
            await avg_stats(last_timestamp)

        last_timestamp = curr_timestamp
        await eval_stats(device_id,value,last_timestamp)


# Calculating min,max,count,sum for each device in each min
async def eval_stats(id,val,time):
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
    for k, v in table.items():
        if k[1] == curr_minute:
            avg = v[3] / v[2]
            await store_to_db([k[0],curr_minute,v[0],v[1],avg])

# Pushing the processed stats to db of each device in each min
async def store_to_db(out):
    print("Store_DB", out)


if __name__ == '__main__':
    app.main()
