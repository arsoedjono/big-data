__author__ = 'Aranda Rizki Soedjono'

from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("local").setAppName("LocationVsYear")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(';')
    year = fields[1]
    locations = fields[2]
    return (year, locations)

lines= sc.textFile("D:/Data/Apps/Canopy/UserFiles/Big-Data/tugas_2/big-data/dataset.dat")
rdd = lines.map(parseLine)

yearLocation = rdd.map(lambda x: ((x[0], x[1]), 1))
yearLocationCount = yearLocation.reduceByKey(add).sortBy(lambda x: -x[1])
results = yearLocationCount.collect()

for result in results:
    print result