import psycopg2
from pymongo import MongoClient
from helper import ChildrenHelper

from os import getenv

# Get connection details from environment variables
postgres_db = getenv("POSTGRES_DB", "postgres")
postgres_user = getenv("POSTGRES_USER", "postgres")
postgres_password = getenv("POSTGRES_PASSWORD", "postgres")
postgres_host = getenv("POSTGRES_HOST", "localhost")
postgres_port = getenv("POSTGRES_PORT", "5432")

# Create an instance of the ChildrenHelper class
postgres_manager = ChildrenHelper(
    postgres_db,
    postgres_user,
    postgres_password,
    postgres_host,
    postgres_port,
)

# Connect to the PostgreSQL database
postgres_manager.connect()

# Retrieve all parent/child tuples from the PostgreSQL database
all_tuples = postgres_manager.get_all_tuples()

# Close the PostgreSQL connection
postgres_manager.close_connection()

# Get connection details from environment variables
mongo_url = getenv("MONGO_URL", "mongodb://localhost:27017")
mongo_db = getenv("MONGO_DB", "default")
mongo_collection = getenv("MONGO_COLLECTION", "default")

# Connect to the MongoDB database
client = MongoClient(mongo_url)
db = client[mongo_db]
collection = db[mongo_collection]

# Update MongoDB documents with children and sibling lists based on retrieved tuples
for parent, child in all_tuples:
    # Update MongoDB documents here based on the parent and child values
    # This is a placeholder, and you should replace it with your actual MongoDB update logic
    collection.update_one(
        {"url": parent},
        {
            "$push": {"children": child},
        },
    )

# For each document in the MongoDB collection, update the siblings list
for document in collection.find():
    # If document don't have parent, skip it
    if "parent" not in document:
        continue
    # Update the siblings list here
    # This is a placeholder, and you should replace it with your actual MongoDB update logic
    collection.update_many(
        {"url": {"$ne": document["url"]}, "parent": document["parent"]},
        {
            "$push": {"siblings": document["url"]},
        },
    )

# Close the MongoDB connection
client.close()

print("Update completed successfully.")
