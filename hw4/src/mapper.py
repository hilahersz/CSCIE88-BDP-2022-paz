#!/usr/bin/env python
"""mapper.py"""
import argparse
import sys

def parse_required_query() -> argparse.Namespace:
    """
    A method to extract required query to run
    """
    parser = argparse.ArgumentParser(prog="emr", description="EMR program")
    parser.add_argument('--query', '-q', required=False, default="q1")
    return parser.parse_args().query


for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for q2_reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print( '%s\t%s' % (word, 1))