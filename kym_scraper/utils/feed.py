import redis


def feed(number_of_pages, redis_url):
    # Create a redis client
    client = redis.from_url(redis_url)

    # Push the urls to the redis queue
    for page in range(1, number_of_pages + 1):
        client.lpush(
            "kym_bootstrap_queue:start_urls",
            f"https://knowyourmeme.com/memes/popular/page/{page}",
        )


if __name__ == "__main__":
    feed(10, "redis://redis:6379")
