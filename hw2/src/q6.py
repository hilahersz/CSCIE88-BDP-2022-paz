import logging
import multiprocessing
from collections import namedtuple
from itertools import groupby
from multiprocessing import Value, Lock

import dateutil.parser

from .q3 import Event, parse_args

logger = logging.getLogger(__name__)

DAY_PATTERN = "%Y-%m-%d"

DistinctTtfb = namedtuple('DistinctTtfb', ['key', 'ttfb'])
TtfbInfo = namedtuple('TtfbInfo', ['key', 'sum', 'count'])
TtfbSummary = namedtuple('TtfbSummary', ['day', 'url', 'ave_ttfb'])

Q6_KEYS = ["2022-09-03", "2022-09-04", "2022-09-05"]


def get_distinct_tuples(shared_counter: Value,
                        shared_list: list,
                        lock: Lock,
                        path: str,
                        ) -> None:
    """
    A method to generate TtfbInfo tuples from a desired file
    Args:
        shared_counter: a shared counter among threads
        shared_list: a shared list of events among threads
        lock: Lock() object for multiprocessing
        path: the file path to process
    """

    logger.info("handling file", extra={"file": path})

    with open(path) as f:
        events = list(map(map_event_to_distinct_day_url, f))
        logger.info("first event recorded", extra={"event": events[0]})
        local_counter = len(events)

        grouped_events = []

        for key, group in groupby(sorted(events, key=lambda x: x.key), lambda x: x.key):
            g = list(group)
            grouped_events.append(TtfbInfo(key, sum([i.ttfb for i in g]), len(g)))
        with lock:
            shared_counter.value += local_counter
            shared_list.append(grouped_events)

        logger.info("added new rows to global counter", extra={"new rows": local_counter})
        logger.info("added new values for global d", extra={"new values": len(grouped_events)})


def map_event_to_distinct_day_url(line):
    """
    A method to parse an event to a date, url and TTFB tuple
    Args:
        line: a single csv file with Event arguments expected

    Returns: parsed DistinctTtfb tuple with a date, url and TTFB

    """
    event = Event(*line.split(','))
    key = dateutil.parser.parse(event.timestamp).strftime(DAY_PATTERN) + " , " + event.url
    return DistinctTtfb(key, float(event.TTFB))


def execute_q6() -> None:
    """
    A custom method to execute query 6 - get top 5 urls per day by lowest ttfb rate
    """
    logger.info("calculating Q5 for question 6")
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
                args=(shared_counter, shared_events, lock, path))

            jobs.append(t)
            t.start()

        for curr_job in jobs:
            curr_job.join()

        # unpack unique values from different threads
        shared_events = [i for x in shared_events for i in x]
        grouped_events = []

        # calculate ttfb
        for key, group in groupby(sorted(shared_events, key=lambda x: x.key), lambda x: x.key):
            g = list(group)
            g_sum = sum([i.sum for i in g])
            g_count = sum([i.count for i in g])
            day = key.split(' ')[0]
            url = key.split(' ')[-1]
            grouped_events.append(TtfbSummary(day, url, g_sum / g_count))

        # keep leading 5 urls per day
        top_events = {}

        for key, group in groupby(sorted(grouped_events, key=lambda x: x.day), lambda x: x.day):
            top_events[key] = sorted(group, key=lambda x: x.ave_ttfb)[:5]

        # print desired answer key
        for k in Q6_KEYS:
            day = top_events[k]
            for i in day:
                answer = f"{k} {i.url} {i.ave_ttfb}"
                logger.info("answer key", extra={"day": k, "answer": answer})

        logger.info("Process Completed", extra={"lines processed": shared_counter.value})


def main():
    logger.info("started main thread")
    execute_q6()
    logger.info("finished main thread")
