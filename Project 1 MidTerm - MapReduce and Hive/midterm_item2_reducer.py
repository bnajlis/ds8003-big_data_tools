#!/usr/bin/env python

import sys

current_movie_id = -1
movie_name = ""
for line in sys.stdin:
	line = line.strip()
	row = line.split('\t')
	if len(row) == 2:
		try:
			# split composite key to recover movie id and row type
			info = row[0].split('_')
			# movie_id is the first part of the composite key
			movie_id = int(info[0])

		except ValueError:
			continue
			
		#if row type is movie name
		if info[1] == 'A':
			try:
				#gets movie name
				movie_name = row[1]
			except ValueError:
				continue
				
		# if row type is movie rating...
		if info[1] == 'B':
			try:
				# get rating 
				rating = int(row[1])
				# print current movie name and rating
				print "%s\t%i" % (movie_name, rating)
			except ValueError:
				continue

			