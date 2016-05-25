__author__ = 'Aranda Rizki Soedjono'

# libraries
from pyspark import SparkConf, SparkContext
from operator import add

# spark configuration
conf = SparkConf().setMaster( "local" ).setAppName( "LocationVsYear" )
sc = SparkContext( conf = conf )

# map function to get year & location from dataset
def parseLine( line ):
  fields = line.split( ';' )
  year = fields[1].encode( 'utf-8' ).strip()
  locations = fields[2].encode( 'utf-8' ).strip().replace( "\"", "" )
  return ( year, locations )

# read file
lines = sc.textFile( "dataset.dat" )
# get each line
rdd = lines.map( parseLine ).filter( lambda x: x[1] != "" )

# map location and year pair to count similar
locationYearCount = rdd.map( lambda x: ( ( x[0], x[1] ), 1 ) )
locationYearSum = locationYearCount.reduceByKey( add )

# re-map, year as key
locationByYear = locationYearSum.map( lambda x: ( x[0][0], ( x[0][1], x[1] ) ) )
# group per year and sort
locationGroupByYear = locationByYear.groupByKey().sortByKey()

# collect result
results = locationGroupByYear.collect()

# print result
length = 0
for result in results:
  max_count = ["", 0]
  for locs in result[1]:
    if locs[1] > max_count[1]:
      max_count = locs
  res = result[0] + ": " + max_count[0]
  print res + " (" + str( max_count[1] ) + ")" if max_count[0] != "" else res
  length = length + 1
print "Jumlah data: " + str( length )
