from pymongo.mongo_client import MongoClient
from pymongo.errors import PyMongoError


class MongoDatabase:
    @staticmethod
    def connect(config):
        try:
            client = MongoClient(
                host=config.MONGO_HOST,
                port=config.MONGO_PORT,
                username=config.MONGO_USER,
                password=config.MONGO_PASSWORD,
                authSource=config.MONGO_AUTH_SOURCE,
                directConnection=True,
                #serverSelectionTimeoutMS=2000,
                #connectTimeoutMS=2000,
                #socketTimeoutMS=5000,
                #retryWrites=True,
                ## connect=False,  # lazy connection
                #appname="ytcomments",
            )
            client[config.MONGO_DB_NAME].command("ping")
            print("Successfully connected to MongoDB!")
        except PyMongoError as e:
            raise RuntimeError(f"Failed to connect to MongoDB: {e}")
        return client