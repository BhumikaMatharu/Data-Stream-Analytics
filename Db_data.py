from pymongo import MongoClient


def get_mongodb_data():
    client = MongoClient('mongodb://localhost:27017')
    db = client['Data_Analysis']
    coll1 = db['Meraki']
    cursor = coll1.find({}, {'_id': 0}).sort([('timestamp_start', 1)])
    for document in cursor:
        print(document)


if __name__ == '__main__':
    get_mongodb_data()
