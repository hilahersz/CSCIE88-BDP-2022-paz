import redis

from .q1 import parse_arguments


def do_work(redis_url):
    redis_client = redis.Redis.from_url(redis_url)


def main():
    parsed_args = parse_arguments()
    redis_counter_name = parsed_args.redis_counter_name
