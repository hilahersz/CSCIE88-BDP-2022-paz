#!/usr/bin/env python3

import sys

keys = dict()
for line in sys.stdin:
    k, v = line.strip().split('\t')
    if keys.get(k) is None:
        keys[k] = set()
    keys[k].add(v)

for k in keys.keys():
    print(f"{k}\t{len(keys[k])}")
