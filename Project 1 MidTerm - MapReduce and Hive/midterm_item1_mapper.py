#!/usr/bin/env python

import sys

for line in sys.stdin:
	line = line.strip()
	movie = line.split('\t')
	if len(movie) == 4:
		try:
			print "%s\t%s" % (movie[2], movie[1])
		except:
			continue