import json
import re
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import nltk
from nltk.corpus import inaugural, stopwords
from pyspark.sql import *

def generate_ngrams(text, stopwords, isToxic, n_gram=1):
    text = re.sub(r'[^\w\s]', '', text)
    token = [token for token in text.lower().split(" ") if token != "" and token not in stopwords]
    ngrams = zip(*[token[i:] for i in range(n_gram)])
    ngramList =  [" ".join(ngram) for ngram in ngrams]
    return [[item, n_gram, isToxic] for item in ngramList]

def extract_hashtags(text):
    hashtags = re.findall(r"#(\w+)", text)

    if len(hashtags) == 0:
        return []
    
    return [[item] for item in hashtags]

def insertMessages(sqlContext):
      df = sqlContext.read.format('csv').options(header='true', inferschema='true').option('quote', '"').option('escape', '"').load('train.csv')
      df = df.selectExpr("question_text as train_message_text", "target as train_message_is_toxic")
      df.write.format('jdbc').options(
            url='jdbc:mysql://127.0.0.1:3306/text_analytics',
            driver='com.mysql.jdbc.Driver',
            dbtable='train_message',
            user='root',
            password='adrian').mode('append').save()


     

def insertNGrams(sc, sqlContext, stopwords, n, csvFileName):
      ngramList = sc.textFile(csvFileName).flatMap(lambda line: generate_ngrams(line[:-2].encode("utf-8"), stopwords, line.split(",")[-1], n)).collect()
      rdd = sc.parallelize(ngramList)
      ngrams = rdd.map(lambda x: Row(train_ngram_text=x[0], train_ngram_type=int(x[1]), train_ngram_is_toxic=int(x[2])))
      df = sqlContext.createDataFrame(ngrams)
      df.write.format('jdbc').options(
            url='jdbc:mysql://127.0.0.1:3306/text_analytics',
            driver='com.mysql.jdbc.Driver',
            dbtable='train_ngram',
            user='root',
            password='adrian').mode('append').save()
      
#some pre-processing on the csv is required. only the column with the text is kept. the initial file was split using split command
#due to memory issues
def insertMultipleNGrams(sc, sqlContext, stopwords):
      csvFileList = ["./train/xaa.csv","./train/xab.csv","./train/xac.csv","./train/xad.csv","./train/xae.csv","./train/xaf.csv"]
      nGrams = [4]
      for n in nGrams:
            for csvFileName in csvFileList:
                  insertNGrams(sc, sqlContext, stopwords, n,csvFileName)

def main(stopwords):

      conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
      sc = SparkContext(conf=conf)
      sqlContext = SQLContext(sc)
      spark = SparkSession.builder.appName("PySpark SQL").getOrCreate()

      # insertMessages(sqlContext)
      insertMultipleNGrams(sc, sqlContext, stopwords)
      
if __name__=="__main__":    
    stopwords = stopwords.words("english")
    main(stopwords)
    