from pyspark import SparkConf, SparkContext         # apache Spark library for configuration & context
from pyspark.mllib.feature import HashingTF, IDF    # TF-IDF library
import re                                           # Regular Expression library
import sys, getopt                                  # (sys) command line features library
                                                    # (getopt) library to parse command line arguments

# data processing function
#
# @param  minFreq   int     minimum similar document(s)
# @param  keyWord   string  keyword to find
#
def mySpark(minFreq, keyWord):

    # text cleaning function
    def removePunctuation(text):
        res=text.lower().strip()
        res=re.sub("[^0-9a-zA-Z ]", "", res)
        return res.split(" ")

    # Function for printing each element in RDD
    def println(x):
        for i in x:
            print i

    # Boilerplate Spark stuff:
    conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
    sc = SparkContext(conf = conf)

    # Load documents content (one per line) + cleaning.
    rawData = sc.textFile("list_berita-30.tsv")
    fields = rawData.map(lambda x: x.split("\t"))
    documents = fields.map(lambda x: removePunctuation(x[3]))

    # Get documents content without word mapping
    documentNames = fields.map(lambda x: x[3])

    # TF processing
    hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
    tf = hashingTF.transform(documents)

    # IDF & TF-IDF processing
    tf.cache()
    idf = IDF(minDocFreq=int(minFreq)).fit(tf)
    tfidf = idf.transform(tf)

    # Get keyword relevance with content and zip it
    keywordTF = hashingTF.transform(removePunctuation(keyWord))
    keywordHashValue = int(keywordTF.indices[0])
    keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])
    zippedResults = keywordRelevance.zip(documentNames)

    # print result
    print "Best document for keywords is:"
    print zippedResults.max()

# main function to process command-line input
def main(argv):
   # variable initialization
   minFreq = ''
   keyWord = ''

   # get command-line arguments
   try:
      opts, args = getopt.getopt(argv,"hm:k:",["minfreq=","keyword="])
   except getopt.GetoptError:
      # syntax error
      print 'tfidf_mod.py -m <minDocFreq> -k <keyWordTF>'
      sys.exit(2)

   # processing input arguments
   for opt, arg in opts:
      # help option -> print syntax
      if opt == '-h':
         print 'tfidf_mod.py -m <minDocFreq> -k <keyWordTF>'
         sys.exit()
      elif opt in ("-m", "--minfreq"):
         minFreq = arg
      elif opt in ("-k", "--keyword"):
         keyWord = arg

   # print input credentials
   print 'Minimum Documents Frequency is ', minFreq
   print 'Key Word TF is ', keyWord

   # call mySpark() function to find keyword relevance in documents
   mySpark(minFreq, keyWord)

if __name__ == "__main__":
   main(sys.argv[1:])
