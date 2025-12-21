from pymongo.mongo_client import MongoClient
from pymongo.errors import PyMongoError
import time


class MongoDatabase:
    @staticmethod
    def connect(config):
        while True:
            try:
                client = MongoClient(
                    host=config.MONGO_HOST,
                    port=config.MONGO_PORT,
                    username=config.MONGO_USER,
                    password=config.MONGO_PASSWORD,
                    authSource=config.MONGO_AUTH_SOURCE,
                    directConnection=True,
                )
                client[config.MONGO_DB_NAME].command("ping")
                print("Successfully connected to MongoDB!")
                return client
            except PyMongoError as e:
                print(f"[ERROR] Failed to connect to MongoDB: {e}. Retrying in 5 seconds...")
                time.sleep(5)