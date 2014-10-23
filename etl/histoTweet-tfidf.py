import re
from pymongo import MongoClient
import math

def histogram(sentence):
	
	if (sentence is None):
		return
	
	words = sentence.lower().split(" ")

	histo = {}

	for i in range(0, len(words)):
		
		word = words[i]
		
		if word.find("http") >= 0:
			continue

		word = re.search(r'[a-zA-Z0-9@#]+', word) 

		if word is None:
			continue

		word = word.group()

		if histo.has_key(word):
			histo[word] += 1
		else:
			histo[word] = 1;

	return histo

def tweetHistogram():
	mg = MongoClient()
	db = mg.twitter

	for tw in db.tweetText.find():
		histo = histogram(tw['tweet'])
		id = tw['_id']

		db.tweetText.update( {"_id": id}, {"$set": {"histo": histo}} )

def tweetIDF():
	mg = MongoClient()
	db = mg.twitter

	total = db.tweetHisto.find().count()

	for th in db.tweetHisto.find():

		if not th.has_key("value"):
			continue

		if not th['value'].has_key("id"):
			continue

		count = len(th['value']['id'])
		try:
			idf = math.log((total/count))
		except:
			idf = 0
		db.tweetHisto.update({"_id": th['_id']}, {"$set": {"idf": idf}} )
		
		print th['_id'], idf
		#print
	

def tfidf():
	mg = MongoClient()
	db = mg.twitter

	idfs = {}

	for i in db.tweetHisto.find():
		if not i.has_key("idf"):
			continue;
		idfs[i['_id']] = i['idf']

	done = 0

	for tw in db.tweetText.find():
		
		if not tw.has_key('histo'):
			continue

		kws = tw['histo']

		if kws is None:
			continue

		tfidf = {}

		for kw in kws:
			tf = kws[kw]
			'''
			idf = db.tweetHisto.find_one({"_id": kw})
			if not idf.has_key("idf"):
				continue

			tfidf[kw] = tf * idf['idf']
			'''
			if not idfs.has_key(kw):
				continue
			
			tfidf[kw] = tf * idfs[kw]

			#print kw,tf,idf['idf']
		
		#print tfidf
		done += 1
		print done
		db.tweetText.update({"_id": tw['_id']}, {"$set": {"tfidf": tfidf}})

def get1000json():
	mg = MongoClient()
	db = mg.twitter

	idfs = {}

	for i in db.tweetText.find():
		print i
	

if __name__ == "__main__":
	#tweetIDF()
	#tfidf()
	get1000json()


