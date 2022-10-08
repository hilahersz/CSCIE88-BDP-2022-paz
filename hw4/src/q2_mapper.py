#!/usr/bin/env python3
import argparse
import sys

from collections import namedtuple
from datetime import datetime

event_fields = ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB']
Event = namedtuple('Event', event_fields)


def parse_line(line):
    return Event._make(line.split(','))


def parse_date(raw_time):
    try:
        raw_time = (raw_time[:-4] + "Z") if len(raw_time) == 30 else raw_time
        event_datetime = datetime.strptime(raw_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        return event_datetime
    except ValueError as e:
        try:
            event_datetime = datetime.strptime(raw_time, '%Y-%m-%dT%H:%M:%SZ')
            return event_datetime
        except ValueError as e:
            return None


def q1_key(parsed_hour, ln):
    return parsed_hour + "\t" + ln.url


def q2_key(parsed_hour, ln):
    return parsed_hour + ":" + ln.url + "\t" + ln.userid


def q3_key(parsed_hour, ln):
    return parsed_hour + ":" + ln.url + "\t" + ln.uuid


KEY_MAP = {"q1": q1_key, "q2": q2_key, "q3": q3_key}

parser = argparse.ArgumentParser()
parser.add_argument('--query_to_run', '-qt', required=False, default="q1")
args = parser.parse_args()

query_to_run = args.query_to_run
key_meth = KEY_MAP.get(query_to_run)

for line in sys.stdin:
    parsed_line = parse_line(line.strip())
    if parsed_line is not None:
        event_datetime = parse_date(parsed_line.timestamp)
        if event_datetime is not None:
            event_hour = datetime.strftime(event_datetime, "%y-%m-%d %H")
            key = key_meth(event_hour, parsed_line)
            print(key)
