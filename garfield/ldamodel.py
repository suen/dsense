#!/usr/bin/python
import sys,os,re,time
import json 
from gensim import corpora, models, similarities
from pymongo import MongoClient


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

	
class TwitterStream:
	def __init__(self):
		words = [ x.strip() for x in open("dictnostops.txt").readlines() ]
		self.dictionary = corpora.Dictionary()
		self.dictionary.add_documents([words])
	
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

	def __iter__(self):
		self.initCursor()
		for d in self.cursor:
			yield self.dictionary.doc2bow(self.clean(d['text']).lower().split())

	#need lazy evaluation for this one
	def enrichDoc(self, model):
		enrichedStream = []
		self.initCursor()
		for d in self.cursor:
			did =  self.dictionary.doc2bow(self.clean(d['text']).lower().split())
			res = model.query(did)
			
			td = []
			for tt in res:
				td.append((model.name + "-" + str(tt[0]), tt[1]))
				
			d['topic'] = td
			print d['text'] + " = ", d['topic']
		return enrichedStream
	
	
mg = TwitterStream()
lda = LDAModel("model1", mg.dictionary)

mg.query("nepal", 0, 10)
mgg = lda.trainModel(mg)

mg.query("nepal", 10, 10)
mgg = lda.trainModel(mg)

mg.query("nepal", 0, 20)
encriched = mg.enrichDoc(lda)

