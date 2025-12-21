import logging
from pymongo.errors import OperationFailure
from pymongo.mongo_client import MongoClient

logger = logging.getLogger("ReplicationInit")


def ensure_replication(config):
    """Check and init MongoDB replication - if need"""
    mongo_url = f"mongodb://{config.MONGO_ADMIN_USER}:{config.MONGO_ADMIN_PASSWORD}" \
                f"@{config.MONGO_HOST}:{config.MONGO_PORT}/{config.MONGO_DB_NAME}?authSource=admin"
    try:
        client = MongoClient(mongo_url)
        logger.info("Connection to MongoDB as admin established.")

        # Check replication status
        try:
            status = client.admin.command("replSetGetStatus")
            logger.info("Replication is already configured: %s", status)
        except OperationFailure as e:
            error_msg = str(e)
            if "not running with --replSet" in error_msg:
                logger.warning("MongoDB is not running with replication (--replSet). Skipping replication setup...")
                return
            elif "not yet initialized" in error_msg:
                logger.warning("Replication set is not initialized. Initializing...")
                client.admin.command("replSetInitiate")
                logger.info("Replication set initialized successfully.")
            else:
                logger.error("Check initialization status error: %s", error_msg)
                raise

    except Exception as e:
        logger.error("Cannot check/initialize replication: %s", e)
        raise
    finally:
        client.close()
        logger.info("MongoDB connection closed.")

