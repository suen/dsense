#!/usr/bin/python
import sys,os,re,time
import json 
from gensim import corpora, models, similarities
from pymongo import MongoClient
from threading import Thread
from datastream import FeedListener
import time
import http

class SuperDictionary:
	def __init__(self):
		words = [ x.strip() for x in open("dictnostops.txt").readlines() ]
		self.dictionary = corpora.Dictionary()
		self.dictionary.add_documents([words])

class ModelWrapper:
	class LDAModel:
		def __init__(self):
			pass

		def setName(self,name):
			self.name = name

		def setDictionary(self, dictionary):
			self.dictionary = dictionary

		def initialize(self):
			self.lda = models.LdaModel(id2word=self.dictionary, num_topics=200) 
		
		def trainModel(self, stream):
			#tfidf = models.TfidfModel(docs);
			#self.corpus_tfidf = tfidf[docs]
			self.lda.update(stream) #let's assume this works

		def query(self, doc):
			return self.lda[doc]

	instance = None

	def __init__(self):
		if ModelWrapper.instance is None:
			ModelWrapper.instance = ModelWrapper.LDAModel()
	
	def getLDAModel(self):
		return ModelWrapper.instance
	
class TwitterMiniBatch:
	def __init__(self):
		self.buffer = []
		self.queue = []
		self.threshold = 10
	
	def setDictionary(self, dictionary):
		self.dictionary = dictionary

	def feed(self, tweetjson):
		if not tweetjson.has_key("text"):
			return	
		self.buffer.append(tweetjson)
		if self.threshold == len(self.buffer):
			self.queue.append(self.buffer)
			self.buffer = []
	
	def hasData(self):
		return (len(self.queue) != 0)
	
	def query(self, collection, start, count):
		self.collection = collection
		self.start = start
		self.count = count
		self.initCursor()

	def initCursor(self):
		self.db = MongoClient().twitter[self.collection]
		self.cursor = self.db.find({"text": {"$exists": True}},{"text":1}).skip(self.start).limit(self.count)
	
	def clean(self, text):
		text = text.replace("RT", "")
		
		hi = text.find("http")
		if hi != -1 :
			text += " "
			si = text.find(" ",hi)
			text = text[0:hi]+text[si:]
			text = text.strip() 
		return text
	
	def queuePop(self):
		self.queue = self.queue[1:]

	def __iter__(self):
		if len(self.queue) == 0:
			return

		for d in self.queue[0]:
			yield self.dictionary.doc2bow(self.clean(d['text']).lower().split())


	#need lazy evaluation for this one
	def enrichDoc(self, model):
		enrichedStream = []
		#self.initCursor()
		for d in self.queue[0]:
			did =  self.dictionary.doc2bow(self.clean(d['text']).lower().split())
			res = model.query(did)
			
			td = []
			for tt in res:
				td.append((model.name + "-" + str(tt[0]), tt[1]))
				
			d['topic'] = td
			#print d['text'] + " = ", d['topic']
			enrichedStream.append(d)
		return enrichedStream

class Main:
	def __init__(self):
		self.model = None

		self.httpThread = Thread(target=self.starthttp)
		self.httpThread.setDaemon(True)
		self.httpThread.start()

	def starthttp(self):
		http.run()
	
	def run(self):
		sdict = SuperDictionary()
		tmb = TwitterMiniBatch()

		tmb.setDictionary(sdict.dictionary)

		self.model = ModelWrapper().getLDAModel()
		self.model.setName("model1")
		self.model.setDictionary(sdict.dictionary)
		self.model.initialize()

		#self.model = LDAModel("model1", sdict.dictionary)

		streamfeed = FeedListener()
		streamfeed.setDataHandler(tmb)
		streamfeed.startListen()

		b = 1
		while True:
			time.sleep(1)
			if not tmb.hasData():
				continue

			print "=========Batch " + str(b) + "=========="
			self.model.trainModel(tmb)

			enr = tmb.enrichDoc(self.model)
			
			streamfeed.publish(enr)

			print str(len(enr)) + " enriched "

			tmb.queuePop()
			b += 1
			print "================== "

	
if __name__ == "__main__":
	main = Main()
	main.run()
