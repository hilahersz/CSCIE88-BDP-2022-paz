import argparse
import logging
import multiprocessing
from collections import namedtuple
from datetime import timedelta
from itertools import groupby
from multiprocessing import Value, Lock

import dateutil.parser

from .q3 import Event, HOUR_PATTERN

logger = logging.getLogger(__name__)

DistinctCountry = namedtuple('DistinctCountry', ['key', 'hour', 'country', 'counter'])

Q5_KEYS = [
    "2022-09-05:19 , AE",
    "2022-09-05:19 , AF",
    "2022-09-05:19 , UG",
    "2022-09-05:23 , PN",
    "2022-09-06:07 , FO",
    "2022-09-06:08 , TK"]


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
    parser.add_argument('--start', '-s', required=False, default="2022-09-05 19:00:00", help="start time")
    parser.add_argument('--end', '-e', required=False, default="2022-09-06 08:00:00", help="end time")

    parsed_args = parser.parse_args()

    logger.info("parsed args", extra={"threads": parsed_args.thread_count, "log_dir": parsed_args.logs_directory})

    return parsed_args.thread_count, parsed_args.logs_directory, parsed_args.start, parsed_args.end


def get_distinct_tuples(shared_counter: Value,
                        shared_list: list,
                        lock: Lock,
                        path: str,
                        ) -> None:
    """
    A method to generate distinct country and hours tuples from a desired file
    Args:
        shared_counter: a shared counter among threads
        shared_list: a shared list of events among threads
        lock: Lock() object for multiprocessing
        path: the file path to process
    """

    logger.info("handling file", extra={"file": path})

    with open(path) as f:
        events = list(map(map_event_to_distinct_country_url, f))
        logger.info("first event recorded", extra={"event": events[0]})
        local_counter = len(events)

        grouped_events = []

        for key, group in groupby(sorted(events, key=lambda x: x.key), lambda x: x.key):
            g = list(group)
            grouped_events.append(DistinctCountry(key, g[0].hour, g[0].country, len(g)))
        with lock:
            shared_counter.value += local_counter
            shared_list.append(grouped_events)

        logger.info("added new rows to global counter", extra={"new rows": local_counter})
        logger.info("added new values for global d", extra={"new values": len(grouped_events)})


def map_event_to_distinct_country_url(line):
    """
    A method to parse an event to an hour, url and country tuple
    Args:
        line: a single csv file with Event arguments expected

    Returns: parsed DistinctCountry tuple with an hour, url and country tuple

    """
    event = Event(*line.split(','))
    hour = dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN)
    key = dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN) + " , " + event.country
    return DistinctCountry(key, hour, event.country, 1)


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
    hours_diff = int(time_diff.seconds / 3600)
    return [(start + timedelta(hours=i)).strftime(HOUR_PATTERN)
            for i in range(hours_diff + 1)]


def execute_q5() -> None:
    """
    A custom method to execute query 4 - country by hour
    """
    logger.info("calculating Q4 for question 5")
    thread_count, logs_dir, start, end = parse_args()

    with multiprocessing.Manager() as manager:
        shared_counter = Value('i', 0)
        shared_events = manager.list()
        lock = Lock()
        jobs = []

        for tid in range(1, thread_count + 1):
            path = f"{logs_dir}file-input{tid}.csv"
            t = multiprocessing.Process(
                target=get_distinct_tuples,
                args=(shared_counter, shared_events, lock, path))

            jobs.append(t)
            t.start()

        for curr_job in jobs:
            curr_job.join()

        # unpack unique values from different threads
        shared_events = [i for x in shared_events for i in x]
        grouped_events = []

        for key, group in groupby(sorted(shared_events, key=lambda x: x.key), lambda x: x.key):
            g = list(group)
            g_count = sum([i.counter for i in g])
            grouped_events.append(DistinctCountry(key, g[0].hour, g[0].country, g_count))

        # print desired answer key
        required_hours = parse_required_hours(start, end)
        grouped_events = [g for g in grouped_events if g.hour in required_hours]

        # Output all countries in time range
        for a in grouped_events:
            print(f"{a.key}: {a.counter}")

        # Explicit answer key
        for k in Q5_KEYS:
            ans = [g for g in grouped_events if g.key == k]
            if len(ans):
                logger.info("answer key", extra={"answer_key": ans[0].key, "answer_count": ans[0].counter})

        logger.info("Process Completed", extra={"lines processed": shared_counter.value})


def main():
    logger.info("started main thread")
    execute_q5()
    logger.info("finished main thread")
