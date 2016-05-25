__author__ = 'Aranda Rizki Soedjono'

from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster( "local" ).setAppName( "LocationVsYear" )
sc = SparkContext( conf = conf )

def parseLine( line ):
  fields = line.split( ';' )
  year = fields[1].encode( 'utf-8' ).strip()
  locations = fields[2].encode( 'utf-8' ).strip().replace( "\"", "" )
  return ( year, locations )

def reduceValue( x, y ):
  print "x = " + str( x )
  return ( x, y )
  # if x[0] == y[0]:
  #   return x[1] + y[1]
  # else:
  #   return x[1]

lines = sc.textFile( "dataset.dat" )
rdd = lines.map( parseLine )

distinctByYear = rdd.distinct()
# locationYearMapValue = distinctByYear.mapValues( lambda x: ( x, 1 ) )
# locationYearMapValue.reduceByKey( lambda x, y: x )

locationYearCount = distinctByYear.map( lambda x: ( ( x[0], x[1] ), 1 ) )
locationYearSum = locationYearCount.reduceByKey( add )

locationByYear = locationYearSum.map( lambda x: ( x[0][0], ( x[0][1], x[1] ) ) )
# locationByYear = locationYearSum.map( lambda x: ( x[0][0], x[1] ) )
# locationYear = locationByYear.reduceByKey( lambda x, y: max( x, y ) )
locationGroupByYear = locationByYear.groupByKey().sortByKey()

results = locationGroupByYear.collect()

length = 0
for result in results:
  max_count = ["", 0]
  for locs in result[1]:
    if locs[1] > max_count[1]:
      max_count = locs
  print result[0] + ": " + max_count[0]
  length = length + 1
print "Jumlah data: " + str( length )
