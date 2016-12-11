#!/usr/bin/env python

from operator import itemgetter
import sys

current_dept = None
current_min_salary = 0
dept = None

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    dept, salary = line.split('\t', 1)

    try:
        salary = int(salary)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue


    if current_dept == dept:
        if salary < current_min_salary:
            current_min_salary = salary
    else:
        if current_dept:
            print '%s\t%s' % (current_dept, current_min_salary)
        current_dept = dept
        current_min_salary = salary

if current_dept == dept:
    print '%s\t%s' % (current_dept, current_min_salary)