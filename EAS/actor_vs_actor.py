__author__ = 'Risky Dwi Setiyawan'

from pyspark import SparkConf, SparkContext
#import collections
from operator import add

conf = SparkConf().setMaster("local").setAppName("ActorVsActor")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(';')
    Title = fields[0]
    Actor1 = fields[8]
    Actor2 = fields[9]
    Actor3 = fields[10]
    return (Title, Actor1, Actor2, Actor3)

filename = "dataset.dat"

lines = sc.textFile(filename)
rdd = lines.map(parseLine)

distinctByTitle = rdd.distinct()
actorMap1 = distinctByTitle.map(lambda x: ((x[1], x[2]), 1))
actorMap2 = distinctByTitle.map(lambda x: ((x[2], x[1]), 1))
actorMap3 = distinctByTitle.map(lambda x: ((x[1], x[3]), 1))
actorMap4 = distinctByTitle.map(lambda x: ((x[3], x[1]), 1))
actorMap5 = distinctByTitle.map(lambda x: ((x[2], x[3]), 1))
actorMap6 = distinctByTitle.map(lambda x: ((x[3], x[2]), 1))

actorActorMap = actorMap1 + actorMap2 + actorMap3 + actorMap4 + actorMap5 + actorMap6

# remove actor == "" , reduce and count (pair) & sort descending by count
# actor 1
ActorActorMap = actorActorMap.filter(lambda x: x[0][0] != "" and x[0][1] != "").reduceByKey(add).sortBy(lambda x: -x[1])

# show 5 most pair director actor
i = 0
for c in ActorActorMap.collect():
    if i == 5:
        break
    print c
    i += 1
