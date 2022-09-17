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

import dateutil.parser
import redis


HOUR_PATTERN = "%Y-%m-%d:%H"


event_fields = ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB']
Event = namedtuple('Event', event_fields)
DistinctUrl = namedtuple('DistinctUrl', ['key', 'url'])
DistinctUser = namedtuple('DistinctUser', ['key', 'user'])
DistinctUuid = namedtuple('DistinctUuid', ['key', 'uuid'])


def parse_arguments() -> argparse.Namespace:
    """
    A method to parse arguments for q1. all arguments have default values
    Returns: a parsed_argument object with the following attributes:
        redis_counter_name: a name to the redis counter
        file_name: a data file (.csv) with data to process
        redis_url: where to locate the redis server
    """
    prog = "counter_process_redis"
    desc = "application that reads a file, parses all lines, counts the lines and " \
           "stores/increments the counter maintained in Redis"

    parser = argparse.ArgumentParser(prog=prog, description=desc)
    # name of a simple String field in Redis - that will be use as a shared counter
    parser.add_argument('--redis_counter_name', '-rc', required=False, default="counter")
    parser.add_argument('--file_name', '-f', required=False, default="src/logs/file-input1.csv",
                        help="a csv log file to process")
    parser.add_argument('--redis_url', '-ru', required=False, default="redis://localhost:6379",
                        help="Redis end point url; Eg: redis://localhost:6379")

    parsed_args = parser.parse_args()
    return parsed_args


def do_work(redis_url, redis_counter_name, file_name) -> None:
    """
    A method to store unique url, users, and uuid per hour in a redis set.
    Args:
        redis_url: The url path to redis host
        redis_counter_name: counter to be updated
        file_name: path of the file to be processed
    """
    redis_client = redis.Redis.from_url(redis_url)
    # set initial value of the redis counter to 0 - if the counter does not exits yet
    event_count = 0

    redis_client.setnx(redis_counter_name, 0)

    with open(file_name) as f:
        file_handle = list(f)
        url_events = list(map(map_event_to_distinct_url, file_handle))
        user_events = list(map(map_event_to_distinct_user, file_handle))
        uuid_events = list(map(map_event_to_distinct_uuid, file_handle))
        for i, e in enumerate(url_events):
            if event_count % 1000 == 0:
                print(f"processing event #{event_count} ... ")
            event_count += 1
            redis_client.incr(redis_counter_name)
            redis_client.sadd(e.key, e.url)
            redis_client.sadd(user_events[i].key, user_events[i].user)
            redis_client.sadd(uuid_events[i].key, uuid_events[i].uuid)

        shared_counter = redis_client.get(redis_counter_name)
        print(f"processing of {file_name} has finished processing with local event_count={event_count}, "
              f"shared counter from Redis: {shared_counter}")

    redis_client.close()


def map_event_to_distinct_url(line):
    """
    A method to parse an event to an hour, url tuple
    Args:
        line: a single csv file with Event arguments expected

    Returns: parsed DistinctUrl tuple with hour and url

    """
    event = Event(*line.split(','))
    ts = dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN)
    return DistinctUrl(ts, event.url)


def map_event_to_distinct_user(line):
    """
    A method to parse an event to an hour, url, userid tuple
    Args:
        line: a single csv file with Event arguments expected

    Returns: parsed DistinctUser tuple with hour, url and userid

    """
    event = Event(*line.split(','))
    key = "user" + dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN) + ":" + event.url
    return DistinctUser(key, event.userid)


def map_event_to_distinct_uuid(line):
    """
    A method to parse an event to an hour, url, uuid tuple
    Args:
        line: a single csv file with Event arguments expected

    Returns: parsed DistinctUser tuple with hour, url and uuid

    """
    event = Event(*line.split(','))
    key = "uuid" + dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN) + ":" + event.url
    return DistinctUuid(key, event.uuid)


def main() -> None:
    """
    A wrapper method to process a .csv file to unique set of url, users and uuid per required key
    As specified in queries 1,2,3
    """
    parsed_args = parse_arguments()
    redis_counter_name = parsed_args.redis_counter_name
    file_name = parsed_args.file_name
    redis_url = parsed_args.redis_url
    do_work(redis_url, redis_counter_name, file_name)


if __name__ == '__main__':
    main()
