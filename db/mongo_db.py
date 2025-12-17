from pymongo.mongo_client import MongoClient
from pymongo.errors import PyMongoError

class MongoDatabase:
    @staticmethod
    async def connect(config):
        try:
            client = MongoClient(
                host=config.MONGO_HOST,
                port=config.MONGO_PORT,
                username=config.MONGO_USER,
                password=config.MONGO_PASSWORD,
                authSource=config.MONGO_AUTH_SOURCE,
                directConnection=True,
                tls=False,
            )
            await client[config.MONGO_DB_NAME].command("ping")
        except PyMongoError as e:
            raise RuntimeError(f"Failed to connect to MongoDB: {str(e)}")
        return client