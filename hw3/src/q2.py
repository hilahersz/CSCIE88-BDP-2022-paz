from collections import namedtuple
from datetime import timedelta

import dateutil.parser
import redis

from .q1 import Event, parse_arguments

HOUR_PATTERN = "%Y-%m-%d:%H"

event_fields = ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB']
DistinctCountry = namedtuple('DistinctCountry', ['key', 'hour', 'url'])


def do_work(redis_url, redis_counter_name, file_name, time_range) -> None:
    """
    A method to store unique url and country.
    Args:
        redis_url: The url path to redis host
        redis_counter_name: counter to be updated
        file_name: path of the file to be processed
        time_range: the time range to record events
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
            if e.hour in time_range:
                redis_client.sadd(e.key, e.url)

        shared_counter = redis_client.get(redis_counter_name)
        print(f"processing of {file_name} has finished processing with local event_count={event_count}, "
              f"shared counter from Redis: {shared_counter}")

    redis_client.close()


def map_event_to_distinct_country_url(line) -> DistinctCountry:
    """
    A method to parse an event to an hour, url and country tuple
    Args:
        line: a single csv file with Event arguments expected

    Returns: parsed DistinctCountry tuple with an hour, url and country tuple

    """
    event = Event(*line.split(','))
    hour = dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN)
    key = dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN) + " , " + event.country
    return DistinctCountry(key, hour, event.url)


def parse_required_hours(start_time, end_time) -> list:
    """
    A method to parse hours range to list of indexes
    Args:
        start_time: required start timestamps
        end_time: required end timestamps

    Returns: a list of keys with desired hours

    """
    start = dateutil.parser.parse(start_time)
    time_diff = dateutil.parser.parse(end_time) - start
    hours_diff = int(time_diff.seconds / 3600 + time_diff.days * 24)
    return [(start + timedelta(hours=i)).strftime(HOUR_PATTERN)
            for i in range(hours_diff + 1)]


def main() -> None:
    """
    A wrapper method to process a .csv file to unique set countries
    Per desired date range, the application prints unique countries per hour
    """
    parsed_args = parse_arguments()
    redis_counter_name = parsed_args.redis_counter_name
    file_name = parsed_args.file_name
    redis_url = parsed_args.redis_url

    start, end = parsed_args.start, parsed_args.end
    hours = parse_required_hours(start, end)

    do_work(redis_url, redis_counter_name, file_name, hours)
