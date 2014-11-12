'''

Usage:
	tfidf.py dbname collection1 collection2
	
	collection1 = collection containing the tweets, tweet.text is assumed as the field containing tweet text
	collection2 = empty collection, used for keyword counting and for the calculation of idf for each keyword

'''
import re
from pymongo import MongoClient
import math
import sys

class ETL:
	
	def __init__(self):
		w = open("stopword.txt").read()
		self.stop = w.split("\n")

	def histogram(self, sentence):
		
		if (sentence is None):
			return None
		
		words = sentence.lower().split(" ")

		histo = {}

		for i in range(0, len(words)):
			
			word = words[i]
			
			if word in self.stop:
				continue

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

	def tweetHistogram(self, collection):
		for tw in collection.find():
			histo = self.histogram(tw['tweet'])
			id = tw['_id']
			if histo is None:
				collection.remove({"_id": id})
			else:
				#print tw['tweet'], histo
				collection.update( {"_id": id}, {"$set": {"histo": histo}} )

	def makeKeywordDictionnary(self, tweetCollection, tweetKWCollection):
		mapjs = '''
			function() {
				var histo = this.histo;

				if (histo) {
					for(w in histo) {
						emit(w.trim(), {count: histo[w], id: [this._id]} );
					}
				}
			}
		'''

		reducejs = '''
			function(word, reducedSet) {
				
				reduced = { count: 0, id: [] };

				for(idx = 0; idx < reducedSet.length; idx++) {
					reduced.count += reducedSet[idx].count++;

					for(ids = 0; ids < reducedSet[idx].id.length; ids++)
						reduced.id.push(reducedSet[idx].id[ids]);
				}

				return reduced;
			}
		'''
		tweetCollection.map_reduce(mapjs, reducejs, tweetKWCollection)


	def tweetIDF(self, collection):
		total = collection.find().count()

		for th in collection.find():
			if not th.has_key("value"):
				continue

			if not th['value'].has_key("id"):
				continue

			count = len(th['value']['id'])
			try:
				idf = math.log((total/count))
			except:
				idf = 0
			collection.update({"_id": th['_id']}, {"$set": {"idf": idf}} )
			#print th['_id'], idf
		

	def tfidf(self, tweetCollection, keywordCollection):
		idfs = {}

		for i in keywordCollection.find():
			if not i.has_key("idf"):
				continue;
			idfs[i['_id']] = i['idf']

		done = 0
		for tw in tweetCollection.find():
			
			if not tw.has_key('histo'):
				continue

			kws = tw['histo']

			if kws is None:
				continue

			tfidf = {}
			idf = {}

			for kw in kws:
				tf = kws[kw]
				if not idfs.has_key(kw):
					continue
				
				tfidf[kw] = tf * idfs[kw]
				idf[kw] = idfs[kw]
				#print kw,tf,idf['idf']
			
			#print tfidf
			done += 1
			#print done
			tweetCollection.update({"_id": tw['_id']}, {"$set": {"tfidf": tfidf, "idf": idf}})

if __name__ == "__main__":
	etl = ETL()

	if ( len(sys.argv) != 4 ):
		sys.exit(-1)
	
	mg = MongoClient()
	db = mg[sys.argv[1]]

	twitterCollection = db[str(sys.argv[2])]
	keywordCollectionstr = sys.argv[3]
	keywordCollection = db[sys.argv[3]]
	
	etl.tweetHistogram(twitterCollection)
	etl.makeKeywordDictionnary(twitterCollection, keywordCollectionstr)
	etl.tweetIDF(keywordCollection)
	etl.tfidf(twitterCollection, keywordCollection)
