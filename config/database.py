import os
from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
import yaml


def load_config():
    """Load configuration from config.yaml."""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)


def get_mongodb_client() -> MongoClient:
    """
    Create and return MongoDB client.

    Returns:
        MongoClient: MongoDB client instance
    """
    config = load_config()
    mongo_config = config['mongodb']

    host = os.getenv('MONGODB_HOST', mongo_config['host'])
    port = int(os.getenv('MONGODB_PORT', mongo_config['port']))

    connection_string = f"mongodb://{host}:{port}/"

    client = MongoClient(connection_string)

    # Test connection
    client.server_info()
    print(f"Connected to MongoDB at {host}:{port}")

    return client


def get_database(client: Optional[MongoClient] = None) -> Database:
    """
    Get database instance.

    Args:
        client: MongoDB client instance (creates new if None)

    Returns:
        Database: MongoDB database instance
    """
    if client is None:
        client = get_mongodb_client()

    config = load_config()
    db_name = os.getenv('MONGODB_DATABASE', config['mongodb']['database'])

    return client[db_name]


def get_collection(collection_name: str,
                   db: Optional[Database] = None) -> Collection:
    """
    Get collection instance.

    Args:
        collection_name: Name of the collection
        db: Database instance (creates new if None)

    Returns:
        Collection: MongoDB collection instance
    """
    if db is None:
        db = get_database()

    config = load_config()
    collections = config['mongodb']['collections']

    # Map logical names to actual collection names
    actual_name = collections.get(collection_name, collection_name)

    return db[actual_name]


def create_indexes(db: Database):
    """
    Create necessary indexes for collections.

    Args:
        db: Database instance
    """
    config = load_config()
    collections = config['mongodb']['collections']

    # Species collection indexes
    species_coll = db[collections['species']]
    species_coll.create_index('key', unique=True)
    species_coll.create_index('scientificName')

    # Observations collection indexes
    obs_coll = db[collections['observations']]
    obs_coll.create_index('key')
    obs_coll.create_index([('location.latitude', 1), ('location.longitude', 1)])
    obs_coll.create_index('timestamp')

    # Classifications collection indexes
    class_coll = db[collections['classifications']]
    class_coll.create_index('audio_file_id')
    class_coll.create_index('key')
    class_coll.create_index('confidence')

    # Audio files collection indexes
    audio_coll = db[collections['audio_files']]
    audio_coll.create_index('minio_path', unique=True)
    audio_coll.create_index('filename')

    print("Database indexes created")
