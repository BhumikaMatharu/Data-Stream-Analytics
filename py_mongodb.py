import motor.motor_asyncio


class MongoClient():

    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
        self.mydb = self.client["TestDB"]
        self.mycol = self.mydb['Output']

    async def insert_one(self, document):
        await self.mycol.insert_one(document)

    async def insert_many(self, documents: list):
        await self.mycol.insert_many(documents)

    async def find_document(self, document):
        doc = await self.mycol.find_one(document)
        return doc

    async def update_document(self,query,modification):
        await self.mycol.replace_one(query,modification)
