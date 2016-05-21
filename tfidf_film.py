__author__ = 'Djuned Fernando Djusdek'

from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF, IDF
import re, os
from sys import platform as _platform

# text cleaning function
def removePunctuation(text):
    res=text.lower().strip()
    res = res.replace("'s ", " ")
    res=re.sub("[^0-9a-zA-Z ]", "", res)
    while(True):
        res=res.replace("  ", " ")
        if "  " not in res:
            break
    return res.split(" ")

# Function for printing each element in RDD
def println(x):
    for i in x:
        print i

# Boilerplate Spark stuff:
conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

# Load documents content (one per line) + cleaning.
dirpath = os.getcwd()
path = ''
if _platform == "win32":
    path = dirpath.replace(dirpath[0:dirpath.index(":")+1], "").replace("\\", "/")
elif _platform == "linux" or _platform == "linux2" or _platform == "darwin":
    path = dirpath

#else
#    path = dirpath

filename = "dataset.dat"

rawData = sc.textFile("file://"+path+"/"+filename)
fields = rawData.map(lambda x: x.split(";"))
documents = fields.map(lambda x: removePunctuation(x[0] + " " + x[1] + " " + x[2] + " " + x[3] + " " + x[4] + " " + x[5] + " " + x[6] + " " + x[7] + " " + x[8] + " " + x[9] + " " + x[10]))

#println(documents.collect())

# Get documents content without word mapping
documentNames = fields.map(lambda x: (x[0] + " " + x[1] + " " + x[2]))

# TF processing
hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
tf = hashingTF.transform(documents)

# IDF & TF-IDF processing
tf.cache()
minFreq = 2
idf = IDF(minDocFreq=int(minFreq)).fit(tf)
tfidf = idf.transform(tf)

# Get keyword relevance with content and zip it
keyWord = "paramount san francisco"
arrayKeyWord = removePunctuation(keyWord)
keywordTF = hashingTF.transform(arrayKeyWord)
for i in range(len(arrayKeyWord)):
    keywordHashValue = int(keywordTF.indices[i])
    keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])
    zippedResults = keywordRelevance.zip(documentNames)
    
    # print result
    print "Best document for keywords " + arrayKeyWord[i] + " is:"
    print zippedResults.max()

#println(tfidf.collect())

