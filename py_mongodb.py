import motor.motor_asyncio


class MongoClient():

    # Initializing Mongo Connection
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
        self.mydb = self.client["Data_Analysis"]
        self.mycol = self.mydb['Devices']

    # Insert one row(document) in DB
    async def insert_one(self, document):
        await self.mycol.insert_one(document)

    # Insert multiple rows(documents) in DB
    async def insert_many(self, documents: list):
        await self.mycol.insert_many(documents)

    # Find the row for given query
    async def find_document(self, document):
        doc = await self.mycol.find_one(document)
        return doc

    # Modify the row(document) for the given query
    async def update_document(self,query,modification):
        await self.mycol.replace_one(query,modification)
