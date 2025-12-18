import os
from dotenv import load_dotenv
from pymongo.errors import PyMongoError

load_dotenv()

class Config:
    def __init__(self):
        self.YTCOMMENTS_HOST = os.getenv("YTCOMMENTS_HOST", "0.0.0.0")
        self.YTCOMMENTS_PORT = int(os.getenv("YTCOMMENTS_PORT", "9093"))

        self.MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
        self.MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
        self.MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "yt_comments")
        self.MONGO_USER = os.getenv("MONGO_USER", "")
        self.MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
        self.MONGO_AUTH_SOURCE = os.getenv("MONGO_AUTH_SOURCE", "yt_comments")