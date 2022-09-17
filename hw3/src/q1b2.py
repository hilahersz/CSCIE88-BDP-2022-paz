import redis

from .q1 import parse_arguments

Q1_KEYS = [
    "2022-09-03:16",
    "2022-09-04:02",
    "2022-09-04:19",
    "2022-09-05:01",
    "2022-09-06:08",
]

Q2_KEYS = [
    "user2022-09-03:16:http://example.com/?url=065",
    "user2022-09-04:02:http://example.com/?url=110",
    "user2022-09-04:19:http://example.com/?url=100",
    "user2022-09-05:01:http://example.com/?url=018",
    "user2022-09-06:08:http://example.com/?url=013",
]

Q3_KEYS = [
    "uuid2022-09-03:16:http://example.com/?url=065",
    "uuid2022-09-04:02:http://example.com/?url=110",
    "uuid2022-09-04:19:http://example.com/?url=100",
    "uuid2022-09-05:01:http://example.com/?url=018",
    "uuid2022-09-06:08:http://example.com/?url=013",
]


def main():
    """
    A wrapper method to print unique counts.
    Prints scard values for desired keys according to the specific question keys
    Assumes that q1b.py was already executed. Otherwise all counts will be 0
    """
    parsed_args = parse_arguments()
    redis_client = redis.Redis.from_url(parsed_args.redis_url)
    for q_keys in [Q1_KEYS, Q2_KEYS, Q3_KEYS]:
        for k in q_keys:
            print(k, redis_client.scard(k))
