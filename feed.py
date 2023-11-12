import redis

NUMBER_OF_PAGES = 5

# Create a redis client
client = redis.from_url("redis://localhost:6379")

# Push the urls to the redis queue
for page in range(1, NUMBER_OF_PAGES + 1):
    client.lpush(
        "kym_bootstrap_queue:start_urls",
        f"https://knowyourmeme.com/memes/popular/page/{page}",
    )
