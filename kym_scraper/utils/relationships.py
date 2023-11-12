import psycopg2
from pymongo import MongoClient
from helper import ChildrenHelper

# Create an instance of the ChildrenHelper class
postgres_manager = ChildrenHelper("airflow", "airflow", "airflow", "localhost", "5432")

# Connect to the PostgreSQL database
postgres_manager.connect()

# Retrieve all parent/child tuples from the PostgreSQL database
all_tuples = postgres_manager.get_all_tuples()

# Close the PostgreSQL connection
postgres_manager.close_connection()

# Connect to the MongoDB database
client = MongoClient("localhost", 27017)
collection = client["airflow"]["memes"]

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
