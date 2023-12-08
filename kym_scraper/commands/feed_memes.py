import redis
from scrapy.commands import ScrapyCommand


class FeedMemes(ScrapyCommand):
    def syntax(self):
        return "<filename>"

    def short_desc(self):
        return "Feed memes URL from file to a redis queue."

    def run(self, args, opts):
        if len(args) != 1:
            raise ValueError("You must provide the filename.")

        filename = args[0]
        print(f"Feeding memes URL from {filename} to the redis queue...")
        client = redis.from_url(self.settings.get("REDIS_URL"))
        with open(filename, "r") as f:
            for line in f.readlines():
                client.lpush("kym_memes_queue:start_urls", line.strip())
        print("Done!")


if __name__ == "__main__":
    FeedMemes().execute()
