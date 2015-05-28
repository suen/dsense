#!/usr/bin/python
import sys,os,re,time
import json 
from gensim import corpora, models, similarities
from pymongo import MongoClient


class LDAModel:
	def __init__(self, dictionary):
		self.lda = models.LdaModel(id2word=dictionary, num_topics=200) 
	
	def trainModel(self, docs):
		#tfidf = models.TfidfModel(docs);
		#self.corpus_tfidf = tfidf[docs]
		self.lda.update(docs)

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
	
mg = TwitterStream()
lda = LDAModel(mg.dictionary)

mg.query("nepal", 0, 10)
mgg = lda.trainModel(mg)

mg.query("nepal", 10, 10)
mgg = lda.trainModel(mg)

mg.query("nepal", 20, 10)
for x in mg:
	print lda.query(x) 

