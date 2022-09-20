from collections import namedtuple
from datetime import timedelta

import dateutil.parser
import redis

from .q1 import Event, parse_arguments

DAY_PATTERN = "%Y-%m-%d"

event_fields = ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB']
Ttfb = namedtuple('DistinctCountry', ['key', 'url', 'ttfb'])


def do_work(redis_url, redis_counter_name, file_name) -> None:
    """
    A method to store unique url and country.
    Args:
        redis_url: The url path to redis host
        redis_counter_name: counter to be updated
        file_name: path of the file to be processed
    """
    redis_client = redis.Redis.from_url(redis_url)
    event_count = 0

    redis_client.setnx(redis_counter_name, 0)

    with open(file_name) as f:
        events = list(map(map_event_to_distinct_country_url, f))
        for e in events:
            if event_count % 1000 == 0:
                print(f"processing event #{event_count} ... ")
            event_count += 1
            redis_client.incr(redis_counter_name)

            # Update TTFB and counter
            redis_client.setnx(e.key+e.url, 0)

            old_count = int(redis_client.get(e.key+e.url))
            old_average = float(redis_client.zscore(e.key, e.url) or 0)
            new_average = (old_average*old_count + e.ttfb) / (old_count + 1)
            redis_client.zadd(e.key, {e.url: new_average})

            redis_client.incr(e.key+e.url)

        shared_counter = redis_client.get(redis_counter_name)
        print(f"processing of {file_name} has finished processing with local event_count={event_count}, "
              f"shared counter from Redis: {shared_counter}")

    redis_client.close()


def map_event_to_distinct_country_url(line) -> Ttfb:
    """
    A method to parse an event to an hour, url and country tuple
    Args:
        line: a single csv file with Event arguments expected

    Returns: parsed DistinctCountry tuple with an hour, url and country tuple

    """
    event = Event(*line.split(','))
    day = dateutil.parser.parse(event.timestamp).strftime(DAY_PATTERN)
    return Ttfb(day, event.url, float(event.TTFB))


def main() -> None:
    """
    A wrapper method to process a .csv file to unique set countries
    Per desired date range, the application prints unique countries per hour
    """
    parsed_args = parse_arguments()
    redis_counter_name = parsed_args.redis_counter_name
    file_name = parsed_args.file_name
    redis_url = parsed_args.redis_url

    do_work(redis_url, redis_counter_name, file_name)
