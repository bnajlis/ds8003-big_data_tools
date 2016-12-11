#!/usr/bin/env python

from operator import itemgetter
import sys
import operator

current_word = None
current_count = 0
word = None
topK = {}
k = 5
# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    word, count = line.split('\t', 1)

    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue


    if current_word == word:
        current_count += count
    else:
        if current_word:
            #print '%s\t%s' % (current_word, current_count)
            if len(topK) < (k):
                topK[current_word] = current_count
            else:
                current_smallest = None
                current_smallest_count = None
                for key in topK:
                    if int(topK[key]) < int(current_count):
                        if current_smallest:
                            if int(topK[key]) < int(current_smallest_count):
                                current_smallest = key
                                current_smallest_count = topK[key]
                        else:
                            current_smallest = key
                            current_smallest_count = topK[key]
                if current_smallest:
                    topK[current_word] = current_count
                    del topK[current_smallest]
        current_count = count
        current_word = word

if current_word == word:
    #print '%s\t%s' % (current_word, current_count)
    if len(topK) < (k):
        topK[current_word] = current_count
    else:
        current_smallest = None
        current_smallest_count = None
        for key in topK:
            if int(topK[key]) < int(current_count):
                if current_smallest:
                    if int(topK[key]) < int(current_smallest_count):
                        current_smallest = key
                        current_smallest_count = topK[key]
                else:
                    current_smallest = key
                    current_smallest_count = topK[key]
        if current_smallest:
            topK[current_word] = current_count
            del topK[current_smallest]
for key in topK:
    print '%s\t%s' % (key, topK[key])
