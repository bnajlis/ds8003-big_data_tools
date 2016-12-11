#!/usr/bin/env python

import sys

for line in sys.stdin:
	line = line.strip()
	row = line.split('\t')
	
	# if movie rating row ...
	if row[0] == "A" and len(row) == 5:
		try:
			# get rating value
			rating = int(row[3])
		except ValueError:
			continue

		# Only output movie ratings with rating > 3
		if rating > 3:
			try:
				# create composite key based on movie id plus "_B"
				# to ensure sort puts ratings AFTER movie names
				
				# print movie_id, rating
				print "%s_B\t%s" % (row[2], row[3])
			except:
				continue

	# if movie name row...
	if row[0] == "B" and len(row) == 4:
		try:
			# create composite key based on movie id plus "_A"
			# to ensure sort puts names BEFORE ratings
			
			# print movie_id, movie_id, rating
			print "%s_A\t%s" % (row[1], row[2])
		except:
			continue