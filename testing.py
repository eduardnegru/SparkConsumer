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
		values = line.split()
		word = values[0]
		coefs = np.asarray(values[1:], dtype='float32')
		embeddings_index[word] = coefs
		if i > 100000:
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
	if rdd.count() != 0:
		data_frame = rdd.toDF(["message_text", "message_is_toxic", "message_created_timestamp", "message_source"])
		data_frame.show()
		data_frame.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database', 'toxic_filtering').option('collection', 'messages').save()

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
	if ("sparkSessionSingletonInstance" not in globals()):
		globals()["sparkSessionSingletonInstance"] = SparkSession \
			.builder \
			.config(conf=sparkConf) \
			.getOrCreate()
	return globals()["sparkSessionSingletonInstance"]


model = load_model('/home/RealTimeToxicityDetection/Ml/ml/model_lstm_on_cpu_adam_binary_crossentropy_wiki')
embeddings_index = load_word_embeddings()


spark = (SparkSession.builder
		.master("local[*]")
		.appName("KerasStream")
		.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/toxic_filtering.messages")
		.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/toxic_filtering.messages")
		.getOrCreate())

sc = spark.sparkContext
ssc = StreamingContext(sc, 3)

modelBroadcast = sc.broadcast(model)
embeddingsBroadcast = sc.broadcast(embeddings_index)
print("Memory leaks")
kafkaParams = {"metadata.broker.list": "localhost:9092"}
directKafkaStream = KafkaUtils.createDirectStream(ssc, ["first_topic"], kafkaParams)

dataStream = directKafkaStream.map(lambda x: json.loads(x[1]))
dataStreamMessage = dataStream.map(lambda messageJSON: [messageJSON["textContent"], modelBroadcast.value.predict(np.array([text_to_array(messageJSON["textContent"], embeddingsBroadcast.value)])).flatten()[0].item(), messageJSON["textCreatedTimestamp"], messageJSON["source"]])
dataStreamMessage.foreachRDD(insertMessages)


ssc.start()
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)
