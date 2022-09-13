import argparse
import logging
import multiprocessing
from collections import namedtuple
from itertools import groupby
from multiprocessing import Value, Lock
from typing import List

import dateutil.parser

logger = logging.getLogger(__name__)


HOUR_PATTERN = "%Y-%m-%d:%H"

Event = namedtuple('Event',
                   ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB'])

DistinctUrl = namedtuple('DistinctUrl', ['key', 'url'])
DistinctUser = namedtuple('DistinctUser', ['key', 'user'])
DistinctUuid = namedtuple('DistinctUuid', ['key', 'uuid'])

Q1_KEYS = ["2022-09-04:07",
           "2022-09-04:08",
           "2022-09-04:09",
           "2022-09-04:10",
           "2022-09-04:11"]

Q2_KEYS = ["2022-09-06:13 , http://example.com/?url=001",
           "2022-09-06:13 , http://example.com/?url=002",
           "2022-09-06:13 , http://example.com/?url=003",
           "2022-09-06:13 , http://example.com/?url=004",
           "2022-09-06:13 , http://example.com/?url=005",
           ]

Q4_KEYS = ["2022-09-05:06 , http://example.com/?url=006",
           "2022-09-05:06 , http://example.com/?url=007",
           "2022-09-05:06 , http://example.com/?url=009",
           "2022-09-05:06 , http://example.com/?url=010",
           "2022-09-05:06 , http://example.com/?url=011",
           ]


def parse_args():
    """
    A method to parse arguments from console
    Returns: parser object with the attributes
        thread_count: how many threads to run
        logs_directory: where the files to read are located

    """
    parser = argparse.ArgumentParser(prog="event_counter_shared_ctypes_mem",
                                     description="run specified number of threads - use shared event counter")

    parser.add_argument('--thread-count', '-tc', default=4, type=int)
    parser.add_argument('--logs-directory', '-ld', required=False, default="hw2/src/logs/",
                        help="Directory where the log files 01-04 are stored. "
                             "default is hw2/src/logs/.")

    parsed_args = parser.parse_args()

    logger.info("parsed args", extra={"threads": parsed_args.thread_count, "log_dir": parsed_args.logs_directory})

    return parsed_args.thread_count, parsed_args.logs_directory


def get_distinct_tuples(shared_counter: Value,
                        shared_list: list,
                        lock: Lock,
                        path: str,
                        method: callable,
                        ) -> None:
    """
    A method to return distinct tuples by keys, with a generated required method
    The method will add unique events to global shared list
    Args:
        shared_counter: a shared counter among threads
        shared_list: a shared list of events among threads
        lock: Lock() object for multiprocessing
        path: the file path to process
        method: the method required for distinct calculations
    """

    logger.info("handling file", extra={"file": path})

    with open(path) as f:
        events = list(map(method, f))
        unique_events = list(set(events))
        logger.info("first event recorded", extra={"event": events[0]})
        local_counter = len(events)

        with lock:
            shared_counter.value += local_counter
            shared_list.append(unique_events)

        logger.info("added new rows to global counter", extra={"new rows": local_counter})
        logger.info("added new values for global d", extra={"new values": len(unique_events)})


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
    key = dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN) + " , " + event.url
    return DistinctUser(key, event.userid)


def map_event_to_distinct_uuid(line):
    """
    A method to parse an event to an hour, url, uuid tuple
    Args:
        line: a single csv file with Event arguments expected

    Returns: parsed DistinctUser tuple with hour, url and uuid

    """
    event = Event(*line.split(','))
    key = dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN) + " , " + event.url
    return DistinctUser(key, event.uuid)


def execute_query(method: callable,
                  answer_key: List[str] = None,
                  ) -> None:
    """
    A wrapper method to execute desired query by key
    Args:
        method: the required method to aggregate by
        answer_key: [Optional], which answer keys to print
    """
    logger.info("calculating queries question 3", extra={"method": method.__name__})
    thread_count, logs_dir = parse_args()

    with multiprocessing.Manager() as manager:
        shared_counter = Value('i', 0)
        shared_events = manager.list()
        lock = Lock()
        jobs = []

        for tid in range(1, thread_count + 1):
            path = f"{logs_dir}file-input{tid}.csv"
            t = multiprocessing.Process(
                target=get_distinct_tuples,
                args=(shared_counter, shared_events, lock, path, method))

            jobs.append(t)
            t.start()

        for curr_job in jobs:
            curr_job.join()

        # unpack unique values from different threads
        unique_events = list(set([i for x in shared_events for i in x]))

        grouped_events = {}

        for key, group in groupby(sorted(unique_events, key=lambda x: x.key), lambda x: x.key):
            grouped_events[key] = len(list(group))

        # print desired answer key
        if answer_key:
            for k in answer_key:
                logger.info("answer key", extra={k: grouped_events[k]})

        logger.info("Process Completed", extra={"lines processed": shared_counter.value})


def main():
    logger.info("started main thread")
    execute_query(map_event_to_distinct_url, Q1_KEYS)
    execute_query(map_event_to_distinct_user, Q2_KEYS)
    execute_query(map_event_to_distinct_uuid, Q4_KEYS)
    logger.info("finished main thread")
