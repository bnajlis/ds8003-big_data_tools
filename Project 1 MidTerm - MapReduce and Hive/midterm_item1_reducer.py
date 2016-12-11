#!/usr/bin/env python

import sys

for line in sys.stdin:
	line = line.strip()
	movie = line.split('\t')
	if len(movie) == 2:
		try:
			rating = int(movie[0])
			movieid = int(movie[1])
		except ValueError:
			continue
		if rating > 3:
			print ("%i\t%i") % (movie_id, rating)