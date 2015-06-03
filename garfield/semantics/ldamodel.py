#!/usr/bin/python
import sys,os,re,time
import json 
from gensim import corpora, models, similarities
from pymongo import MongoClient
from threading import Thread
from datastream import FeedListener
import time


class LDAModel:
	def __init__(self, name, dictionary):
		self.name = name
		self.lda = models.LdaModel(id2word=dictionary, num_topics=200) 
	
	def trainModel(self, stream):
		#tfidf = models.TfidfModel(docs);
		#self.corpus_tfidf = tfidf[docs]
		self.lda.update(stream) #let's assume this works

	def query(self, doc):
		return self.lda[doc]

	
class TwitterMiniBatch:
	def __init__(self):
		words = [ x.strip() for x in open("dictnostops.txt").readlines() ]
		self.dictionary = corpora.Dictionary()
		self.dictionary.add_documents([words])
		self.buffer = []
		self.queue = []
		self.threshold = 10

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


		"""
		self.initCursor()
		for d in self.cursor:
			yield self.dictionary.doc2bow(self.clean(d['text']).lower().split())
		"""

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
	

def dataFeedThread():
	x = 0
	while True:
		time.sleep(5)
		mg.query("nepal", x, 5)
		x += 19
		for d in mg.cursor:
			mg.feed(d)
	
#thread = Thread(target=dataFeedThread)
#thread.setDaemon(True)
#thread.start()

mg = TwitterMiniBatch()
lda = LDAModel("model1", mg.dictionary)

streamfeed = FeedListener()
streamfeed.setDataHandler(mg)
streamfeed.startListen()

b = 1
while True:
	time.sleep(1)
	
	if not mg.hasData():
		continue

	print "=========Batch " + str(b) + "=========="
	lda.trainModel(mg)

	enr = mg.enrichDoc(lda)
	
	streamfeed.publish(enr)

	#for e in enr:
	#	print e," to redis"
	print str(len(enr)) + " enriched "
	'''
	l = 0
	for x in mg:
		print x
		l += 1
	'''
	mg.queuePop()

	b += 1
	print "================== "

exit()


"""
mg.query("nepal", 0, 10)
mgg = lda.trainModel(mg)

mg.query("nepal", 10, 10)
mgg = lda.trainModel(mg)

mg.query("nepal", 0, 20)
encriched = mg.enrichDoc(lda)
"""
