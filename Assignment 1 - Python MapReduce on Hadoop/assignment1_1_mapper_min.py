#!/usr/bin/env python

import sys

for line in sys.stdin:
    line = line.strip()
    salary = line.split()
    if len(salary) == 2:
        try:
            print '%s\t%s' % (salary[0], salary[1])
        except:
            continue