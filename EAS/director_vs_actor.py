__author__ = 'Djuned Fernando Djusdek'

from pyspark import SparkConf, SparkContext
#import collections
from operator import add
import os
from sys import platform as _platform

conf = SparkConf().setMaster("local").setAppName("DirectorVsActor")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(';')
    Title = fields[0]
    Director = fields[6]
    Actor1 = fields[8]
    Actor2 = fields[9]
    Actor3 = fields[10]
    return (Title, Director, Actor1, Actor2, Actor3)

#path = "/Users/user/Documents/GitHub/big-data"

dirpath = os.getcwd()
path = ''
if _platform == "win32":
    path = dirpath.replace(dirpath[0:dirpath.index(":")+1], "").replace("\\", "/")
elif _platform == "linux" or _platform == "linux2" or _platform == "darwin":
    path = dirpath

#else
#    path = dirpath

filename = "dataset.dat"

lines = sc.textFile("file://"+path+"/"+filename)
rdd = lines.map(parseLine)

distinctByTitle = rdd.distinct()
directorActorMap1 = distinctByTitle.map(lambda x: ((x[1], x[2]), 1))
directorActorMap2 = distinctByTitle.map(lambda x: ((x[1], x[3]), 1))
directorActorMap3 = distinctByTitle.map(lambda x: ((x[1], x[4]), 1))

directorActorMap = directorActorMap1 + directorActorMap2 + directorActorMap3

#                                   remove actor == ""             reduce and count (pair) & sort descending by count
directorActorMap = directorActorMap.filter(lambda x: x[0][1] != "").reduceByKey(add).sortBy(lambda x: -x[1])

'''
print len(directorActorMap.collect())
print len(directorActorMap1.collect())
print len(directorActorMap2.collect())
print len(directorActorMap3.collect())
'''

'''
for c in directorActorMap.collect():
    print c
'''

# show 5 most pair director actor
i = 0
for c in directorActorMap.collect():
    if i == 5:
        break
    print c
    i += 1
