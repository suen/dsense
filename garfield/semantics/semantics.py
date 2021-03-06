#!/usr/bin/python
import sys,os,re,time
import json 
from gensim import corpora, models, similarities
from pymongo import MongoClient
from threading import Thread
from datastream import FeedListener
from dictionary import WrapperDictionary
import web 
from patterns import Singleton


class LDAModel:
	def __init__(self):
		pass

	def setName(self,name):
		self.name = name

	def setDictionary(self, dictionary):
		self.dictionary = dictionary

	def initialize(self):
		self.lda = models.LdaModel(id2word=self.dictionary, num_topics=200) 
	
	def saveModel(self, filename):
		self.lda.save(filename)
	
	def loadFromFile(self, filename):
		self.lda = self.lda.load(filename)
	
	def trainModel(self, stream):
		#tfidf = models.TfidfModel(docs);
		#self.corpus_tfidf = tfidf[docs]
		self.lda.update(stream) #let's assume this works

	def query(self, doc):
		return self.lda[doc]
	
	def textquery(self,text):
		text = text.lower().split()
		doc = self.dictionary.doc2bow(text)
		results = self.lda[doc]
		topics = []
		for result in results:
			topics.append({"model": self.name + "-" + str(result[0]), "score": result[1]})
		return topics 

	def show_topics(self, nb_topics=10):
		return self.lda.show_topics(num_topics=nb_topics)
	
class TwitterMiniBatch:
	def __init__(self, buffersize):
		self.buffer = []
		self.queue = []
		self.buffersize = buffersize 
	
	def setDictionary(self, dictionary):
		self.dictionary = dictionary

	def feed(self, tweetjson):
		if not tweetjson.has_key("text"):
			return	
		self.buffer.append(tweetjson)
		if self.buffersize == len(self.buffer):
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
				td.append({"model": model.name + "-" + str(tt[0]), "score": tt[1]})
				
			d['topic'] = td
			#print d['text'] + " = ", d['topic']
			enrichedStream.append(d)
		return enrichedStream

@Singleton
class ModelDict:

	def init(self, modelname, nbWords):
		wdict = WrapperDictionary()
		wdict.init(nbWords)

		self.model = LDAModel()
		self.model.setName(modelname)
		self.model.setDictionary(wdict)
		self.model.initialize()

		self.dict = wdict
	

