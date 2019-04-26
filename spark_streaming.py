import json
import re
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
import nltk
from nltk.corpus import inaugural, stopwords
from tqdm import tqdm
import os
import numpy as np
import pandas as pd
import math
import pickle
import json
from sklearn.model_selection import train_test_split
from keras.models import Sequential, load_model
from keras.layers import CuDNNLSTM, Dense, Bidirectional, LSTM
from sklearn.externals import joblib
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from tqdm import tqdm
from keras.models import model_from_json

# Convert values to embeddings
def text_to_array(text, broadcastEmbedding):
	empyt_emb = np.zeros(300)
	text = text[:-1].split()[:30]
	embeds = [broadcastEmbedding.get(x, empyt_emb) for x in text]
	embeds += [empyt_emb] * (30 - len(embeds))
	return np.array(embeds)


def load_word_embeddings():
	embeddings_index = {}
	
	embeddings_file_path = '/home/adrian/Desktop/quora/wiki/wiki.vec'
	f = open(embeddings_file_path)
	i = 0
	print("READING WORD EMBEDDINGS !!!!")
	for line in tqdm(f):
		values = line.split(" ")
		word = values[0]
		coefs = np.asarray(values[1:], dtype='float32')
		embeddings_index[word] = coefs
		if i > 10000:
			break
		i += 1
	f.close()

	return embeddings_index

def generate_ngrams(text, stopwords, n_gram=1):
	text = re.sub(r'[^\w\s]', '', text)
	token = [token for token in text.lower().split(" ") if token != "" and token not in stopwords]
	ngrams = zip(*[token[i:] for i in range(n_gram)])
	ngramList =  [" ".join(ngram) for ngram in ngrams]
	return [[item, n_gram] for item in ngramList]

def extract_hashtags(text):
	hashtags = re.findall(r"#(\w+)", text)

	if len(hashtags) == 0:
		return []

	return [[item] for item in hashtags]

def insertMessages(rdd):
	data_frame = rdd.toDF(["message_text", "message_is_toxic"])
	data_frame.show()
	data_frame.write.format('jdbc').options(
	url='jdbc:mysql://127.0.0.1:3306/text_analytics?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',
	driver='com.mysql.jdbc.Driver',
	dbtable='message',
	user='root',
	password='adrian').mode('append').save()


def insertHashtags(rdd):
	if rdd.count() == 0:
		return

	data_frame = rdd.toDF(['hashtag_text'])
	data_frame.show()
	data_frame.write.format('jdbc').options(
	url='jdbc:mysql://127.0.0.1:3306/text_analytics',
	driver='com.mysql.jdbc.Driver',
	dbtable='hashtag',
	user='root',
	password='adrian').mode('append').save()

def insertNGrams(rdd):
	data_frame = rdd.toDF(['ngram_text', 'ngram_type'])
	data_frame.show()
	data_frame.write.format('jdbc').options(
	url='jdbc:mysql://127.0.0.1:3306/text_analytics',
	driver='com.mysql.jdbc.Driver',
	dbtable='ngram',
	user='root',
	password='adrian').mode('append').save()


# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
	if ("sparkSessionSingletonInstance" not in globals()):
		globals()["sparkSessionSingletonInstance"] = SparkSession \
			.builder \
			.config(conf=sparkConf) \
			.getOrCreate()
	return globals()["sparkSessionSingletonInstance"]

# def get_pred(data):
	
	# loaded_model = model_from_json(data_model.value)
	# loaded_model.load_weights("/home/RealTimeToxicityDetection/SparkConsumer/CNN_model.h5")
	# return loaded_model.predict(np.array([text_to_array("hello dear dad", embeddingsBroadcast.value)])).flatten()[0]
	# return 1 if loaded_model.predict(np.zeros((1, 15, 300))) > 0.5 else 0

	# return 1 if model.predict(np.array([text_to_array("Donald Trump is a fucking asshole", embeddingsBroadcast.value)])).flatten()[0] > 0.5 else 0
	
def predict(rdd):

	spark = getSparkSessionInstance(rdd.context.getConf())
	rowRdd4 = rdd.map(lambda w: Row(sen=get_pred(w)))
	df4 = spark.createDataFrame(rowRdd4)
	df4.show()



# def main(stopwords):

# 	conf = SparkConf().setMaster("local[*]").setAppName("SparkStreamingFromKafka")
# 	sc = SparkContext(conf=conf)
# 	sc.setLogLevel("ERROR")
# 	spark = SparkSession(sc)

# 	# Creating a streaming context with batch interval of 10 sec
# 	ssc = StreamingContext(sc, 10)
# 	ssc.checkpoint("checkpoint")

# 	embeddings_index = load_word_embeddings()
# 	model = load_model('/home/RealTimeToxicityDetection/SparkConsumer/model')

# 	modelBroadcast = sc.broadcast(model)
# 	embeddingsBroadcast = sc.broadcast(embeddings_index)
	
# 	prediction = 1 if modelBroadcast.value.predict(np.array([text_to_array("hello dear dad", embeddingsBroadcast.value)])).flatten()[0] > 0.5 else 0
# 	print("Broadcasting model")

# 	kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['first_topic'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})

# 	dataStream = kafkaStream.map(lambda x: json.loads(x[1]))
# 	dataStreamMessage = dataStream.map(lambda x: x["textContent"])

# 	# dataStreamMessageData = dataStreamMessage.map(lambda message: [message, 1 if modelBroadcast.value.predict(np.array([text_to_array("hello dear dad", embeddingsBroadcast.value)])).flatten()[0] > 0.5 else 0])
# 	dataStreamMessageData = dataStreamMessage.map(lambda message: [message, 1])
# 	# pred  = 1 if modelBroadcast.value.predict(np.array([text_to_array("hello dear dad", embeddingsBroadcast.value)])).flatten()[0] > 0.5 else 0

# 	# dataPred = dataStreamMessage.map(lambda message: predict(message))
# 	dataStreamMessageData.pprint()
# 	# dataPred.foreachRDD(printPred)
# 	ssc.start()
# 	ssc.awaitTermination()
# 	ssc.stop(stopGraceFully = True)




	# dataStreamMessageData = dataStreamMessage.map(lambda message: [message, 1])
	# dataStreamMessageData = dataStreamMessage.map(lambda message: message)

	# dataStreamHashtags = dataStreamMessage.flatMap(lambda x: extract_hashtags(x))
	# dataStreamHashtags = dataStreamHashtags.filter(lambda x: x is not [])

	# dataStreamOneGram = dataStreamMessage.flatMap(lambda x: generate_ngrams(x, stopwords, 1))
	# dataStreamTwoGram = dataStreamMessage.flatMap(lambda x: generate_ngrams(x, stopwords, 2))
	# dataStreamThreeGram = dataStreamMessage.flatMap(lambda x: generate_ngrams(x, stopwords, 3))

	# stream = dataStreamOneGram.union(dataStreamTwoGram).union(dataStreamThreeGram)

	# dataStreamMessageData.foreachRDD(insertMessages)
	# dataStreamHashtags.foreachRDD(insertHashtags)
	# stream.foreachRDD(insertNGrams)


# model = load_model('/home/RealTimeToxicityDetection/SparkConsumer/model')
model = load_model('/home/RealTimeToxicityDetection/Ml/ml/model_simple_nn')
embeddings_index = load_word_embeddings()


spark = (SparkSession.builder
		.master("local[*]")
		.appName("KerasStream")
		.getOrCreate() )

sc = spark.sparkContext
ssc = StreamingContext(sc, 3)

modelBroadcast = sc.broadcast(model)
embeddingsBroadcast = sc.broadcast(embeddings_index)

kafkaParams = {"metadata.broker.list": "localhost:9092"}
directKafkaStream = KafkaUtils.createDirectStream(ssc, ["first_topic"], kafkaParams)

dataStream = directKafkaStream.map(lambda x: json.loads(x[1]))
dataStreamMessage = dataStream.map(lambda x: x["textContent"])
dataStreamMessage = dataStreamMessage.map(lambda message: [message, 1 if modelBroadcast.value.predict(np.array([text_to_array(message, embeddingsBroadcast.value)])).flatten()[0] > 0.5 else 0])
dataStreamMessage.foreachRDD(insertMessages)

ssc.start()
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)
