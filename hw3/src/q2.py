# Copyright (c) 2022 CSCIE88 Marina Popova
'''
This is a very simple Python application that reads one file, parses each line per the specified schema
(event_fields), and counts the number of lines in the file - by incrementing it's local counter (event_count);
It also increments a shared counter maintained in Redis - by 1 for each line - so that after all instances
of this application are done processing their own files - we have a total count of lines available in Redis.

In this app - we chose to increment the shared Redis counter per each processed line - to see the running total count
in progress in Redis; One could choose to increment the shared counter only once, when all line are counted locally -
to decrease the number of calls to Redis. However, in this approach - the shared counter will not show the running total
'''
import argparse
from collections import namedtuple
from datetime import timedelta

import dateutil.parser
import redis

from .q1 import Event

HOUR_PATTERN = "%Y-%m-%d:%H"

event_fields = ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB']
DistinctCountry = namedtuple('DistinctCountry', ['key', 'hour', 'url'])

Q4_KEYS = [
    "2022-09-04:03 , KI",
    "2022-09-04:10 , DJ",
    "2022-09-05:01 , AS",
    "2022-09-05:16 , CH",
    "2022-09-05:20 , VE",

]


def parse_arguments() -> argparse.Namespace:
    """
    A method to parse arguments for q2. all arguments have default values
    Returns: a parsed_argument object with the following attributes:
        redis_counter_name: a name to the redis counter
        file_path: a data folder with all .csv files to process
        redis_url: where to locate the redis server
        start: when to start printing hours and country combinations
        end: when to stop printing hours and country combinations
    """
    prog = "counter_process_redis"
    desc = "application that reads a file, parses all lines, counts the lines and " \
           "stores/increments the counter maintained in Redis"

    parser = argparse.ArgumentParser(prog=prog, description=desc)
    # name of a simple String field in Redis - that will be use as a shared counter
    parser.add_argument('--redis_counter_name', '-rc', required=False, default="counter")
    parser.add_argument('--file_name', '-f', required=False, default="src/logs/file-input1.csv",
                        help="a path with csv log files to process")
    parser.add_argument('--redis_url', '-ru', required=False, default="redis://localhost:6379",
                        help="Redis end point url; Eg: redis://localhost:6379")
    parser.add_argument('--start', '-s', required=False, default="2022-09-04 2 AM UTC", help="start time")
    parser.add_argument('--end', '-e', required=False, default="2022-09-05 10 PM UTC", help="end time")

    parsed_args = parser.parse_args()
    return parsed_args


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
    # set initial value of the redis counter to 0 - if the counter does not exits yet
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
    hours_diff = int(time_diff.seconds / 3600 +time_diff.days * 24)
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
