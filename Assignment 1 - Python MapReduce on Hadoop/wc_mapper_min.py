#!/usr/bin/env python

import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    if len(words) == 2:
        try:
            print '%s\t%s' % (words[0], words[1])
        except:
            continue