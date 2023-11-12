import psycopg2
from pymongo import MongoClient
from kym_scraper.spiders.helper import ChildrenHelper

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

    # Find parent document and print it
    print(collection.find_one({"url": parent}))

# Close the MongoDB connection
client.close()

print("Update completed successfully.")
