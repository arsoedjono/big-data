from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF, IDF
import re
import sys, getopt

def mySpark(minFreq, keyWord):
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

    # Load documents (one per line).
    rawData = sc.textFile("list_berita-30.tsv")
    fields = rawData.map(lambda x: x.split("\t"))
    documents = fields.map(lambda x: removePunctuation(x[3]))

    #println( documents.collect() )

    documentNames = fields.map(lambda x: x[3])

    hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
    tf = hashingTF.transform(documents)

    tf.cache()
    idf = IDF(minDocFreq=int(minFreq)).fit(tf)
    tfidf = idf.transform(tf)

    #print keyWord

    keywordTF = hashingTF.transform(removePunctuation(keyWord))
    keywordHashValue = int(keywordTF.indices[0])

    keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])

    zippedResults = keywordRelevance.zip(documentNames)

    print "Best document for keywords is:"
    print zippedResults.max()

def main(argv):
   minFreq = ''
   keyWord = ''
   try:
      opts, args = getopt.getopt(argv,"hm:k:",["minfreq=","keyword="])
   except getopt.GetoptError:
      print 'tfidf_mod.py -m <minDocFreq> -k <keyWordTF>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'tfidf_mod.py -m <minDocFreq> -k <keyWordTF>'
         sys.exit()
      elif opt in ("-m", "--minfreq"):
         minFreq = arg
      elif opt in ("-k", "--keyword"):
         keyWord = arg
   print 'Minimun Documents Frequency is ', minFreq
   print 'Key Word TF is ', keyWord

   mySpark(minFreq, keyWord)

if __name__ == "__main__":
   main(sys.argv[1:])
