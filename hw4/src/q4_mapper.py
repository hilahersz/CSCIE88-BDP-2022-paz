#!/usr/bin/env python3
import argparse
import sys
import dateutil.parser
from datetime import timedelta


from collections import namedtuple

HOUR_PATTERN = "%Y-%m-%d:%H"

event_fields = ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB']
Event = namedtuple('Event', event_fields)
DistinctCountry = namedtuple('DistinctCountry', ['hour', 'key', 'url'])


def parse_line(line):
    event = Event._make(line.split(','))
    hour = dateutil.parser.parse(event.timestamp).strftime(HOUR_PATTERN)
    key = hour + " , " + event.country
    return DistinctCountry(hour, key, event.url)


def parse_required_hours(start_time, end_time):
    start = dateutil.parser.parse(start_time)
    time_diff = dateutil.parser.parse(end_time) - start
    hours_diff = int(time_diff.seconds / 3600 + time_diff.days * 24)
    return [(start + timedelta(hours=i)).strftime(HOUR_PATTERN)
            for i in range(hours_diff + 1)]


parser = argparse.ArgumentParser()
parser.add_argument('--start', '-s', required=False, default="2022-09-05 19:00")
parser.add_argument('--end', '-e', required=False, default="2022-09-06 08:00")
args = parser.parse_args()

time_range = parse_required_hours(args.start, args.end)

for line in sys.stdin:
    parsed_line = parse_line(line.strip())
    if parsed_line is not None:
        if parsed_line.hour in time_range:
            print(f"{parsed_line.key}\t{parsed_line.url}")
