#!/usr/bin/env python

import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split(",")
    if len(words) == 3:
        try:
            if words[1] == "QA" or words[2] == "Math":
                print line
        except:
            continue
