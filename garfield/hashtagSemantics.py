#!/usr/bin/python
import sys,os,re
import json 
from gensim import corpora, models, similarities
from pymongo import MongoClient

class SemanticClustering:
	def __init__(self, dictFilename=""):
		#initialize stopword list
		self.stopwords = open("stopword.txt","r").read().strip().split("\n")
		self.stopwords = [ sw for sw in self.stopwords if sw.strip() !="" ]

		#initialize corpora.Dictionary
		self.dictFile = dictFilename if dictFilename!="" else "dictionary.dict" 
		self.corpusDict = corpora.Dictionary()
		if not os.path.isfile(self.dictFile):
			self.corpusDict.save(self.dictFile)
		else:
			self.corpusDict.load(self.dictFile)

		#regex patterns
		self.tokenpattern = re.compile(r'[@_0-9a-z\/]+')

		self.notaword = "RT|http|@".split("|")
	
	def saveDictionnary(self):
		self.corpusDict.save(self.dictFile)

	def tweetWordFilter(self, word):
		for n in self.notaword:
			if word.find(n) >= 0:
				return ""

		if self.tokenpattern.match(word) is None:
			return ""

		word = word.strip()
		if word in self.stopwords:
			return ""

		return word
	
	def applyTransformation(self,corpus, model="LSA"):
		if model == "LSA":
			pass

		if model == "LDA":
			pass

	def clustering(self,tweets,nbTopics):
		tweettokens = []
		for tweet in tweets:
			tweet = tweet[0].replace("\"","").replace("RT ", "").replace("(", " ").replace(")"," ")
			tws = tweet.lower().split(" ")
			tws = [i for i in tws if self.tweetWordFilter(i) != "" ] 
			tweettokens.append(tws)

		all_tokens = sum(tweettokens, [])
		tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
		tweettokens = [[word for word in tweet if word not in tokens_once] for tweet in tweettokens]


		self.corpusDict.add_documents(tweettokens)
		corpus = [self.corpusDict.doc2bow(tweet) for tweet in tweettokens]
		tfidf = models.TfidfModel(corpus)
		corpus_tfidf = tfidf[corpus] 

		all_len = 0
		for doc in corpus_tfidf:
			all_len += len(doc)

		if all_len ==0:
			return;

		#for doc in corpus_tfidf:
		#	print doc

		lsi = models.LsiModel(corpus_tfidf, id2word=self.corpusDict, num_topics=nbTopics) # initialize an LSI transformation
		corpus_lsi = lsi[corpus_tfidf] # create a double wrapper over the original corpus: bow->tfidf->fold-in-lsi

		#print "==========Topics====================="
		clusters = []
		ty = 0
		tweetTopicGrouped = {}
		for doc in corpus_lsi: # both bow->tfidf and tfidf->lsi transformations are actually executed here, on the fly
			max = None
			for atuple in doc:
				if max is None:
					max = atuple
					continue;
				if max[1] < atuple[1]:
					max = atuple
			
			if max is not None:
				max_topic = str(max[0])
				if not tweetTopicGrouped.has_key(max_topic):
					tweetTopicGrouped[max_topic] = []

				tweetTopicGrouped[max_topic].append((max[1], tweets[ty][2],tweets[ty][0]))	

			clusters.append({"tweet": tweets[ty], "main": max,  "topics": doc})
			ty += 1
		self.saveDictionnary()
		return clusters,tweetTopicGrouped;
	
	def test_clustering(self, collection, num_topics, file):
		dbcollections = MongoClient().twitter[collection]
		tweets = []
		count = 0
		unique_hashtags = set()
		allcount = 0
		for tweet in dbcollections.find({}, {"_id": 1, "entities": 1, "text": 1}):
			allcount += 1
			if not tweet.has_key('text'):
				continue

			hashtags = [] 
			if tweet.has_key('entities') and tweet['entities'].has_key('hashtags')  > 0:
				for i in range(len(tweet['entities']['hashtags'])):
					atag = tweet['entities']['hashtags'][i]['text']
					hashtags.append(atag)
					unique_hashtags.add(atag)

				if len(hashtags) == 0:
					continue

				tweets.append((tweet['text'], tweet['_id'], hashtags))

				count += 1
				if count == 10000:
					break;
			#print tweet['text']
			#print hashtags
		#print "Tweets in the corpus:", len(tweets)
		#print "Unique hashtags : ", len(unique_hashtags)


		nb_topics_to_detect = num_topics 
		tweetlabelled, topicCluster = self.clustering(tweets, nb_topics_to_detect)

		#for i in range(len(tweetlabelled)):
		#	print str(tweetlabelled[i]) + "\n"

		"""
		for i in topicCluster:
			for tweet in topicCluster[i]:
				print tweet[0],str(tweet[1]),tweet[2].encode("utf-8", "ignore")
		"""


		#print "=============Purity=============="
		nbTweets = 0
		for t in topicCluster:
			nbTweets += len(topicCluster[t])


		purity_sum = 0
		for t in topicCluster:
			#print
			#print "=========================="
			#print "Topic #", t 
			#print "---------------------"
			tags = {}
			for tweetTopic in topicCluster[t]:
				#print tweetTopic
				hashtags = tweetTopic[1]
				for i in range(len(hashtags)):
					if tags.has_key(hashtags[i]):
						tags[hashtags[i]] += 1
					else:
						tags[hashtags[i]] = 1
			
			max = 0
			for i in tags:
				if tags[i] > max:
					max = tags[i]

			total = len(topicCluster[t])
			purity = float(float(max) / float(total))
			normalized = purity * (float(float(total)/float(nbTweets)))
			purity_sum += normalized
			#print "---------------------"
			#print "Topic purity:", round(purity,2), normalized
			#print "Purity for topic "  + str(t) +"(" +str(max) + "/" + str(total) + ") : "  + str(purity)
			#print topicCluster[t]
			#print "Topic " + str(t) +"\t"+ str(normalized)
		#print
		#print
		#print "============================="
		#print
		#print "Total topics detected: " + str(len(topicCluster))  + "/" + str(nb_topics_to_detect)
		#print "Nb Tweets: " + str(nbTweets) + " tweets"
		#print "Global purity: " + str(purity_sum)
		#print

		return len(unique_hashtags), len(topicCluster), nbTweets, purity_sum

	def run_clustering(self, tweets, nb):
		tweetlabelled, topicCluster = self.clustering(tweets, nb)

		#print "=============Purity=============="
		nbTweets = 0
		for t in topicCluster:
			nbTweets += len(topicCluster[t])

		purity_sum = 0
		for t in topicCluster:
			tags = {}
			for tweetTopic in topicCluster[t]:
				#print tweetTopic
				hashtags = tweetTopic[1]
				for i in range(len(hashtags)):
					if tags.has_key(hashtags[i]):
						tags[hashtags[i]] += 1
					else:
						tags[hashtags[i]] = 1
			
			max = 0
			for i in tags:
				if tags[i] > max:
					max = tags[i]

			total = len(topicCluster[t])
			purity = float(float(max) / float(total))
			normalized = purity * (float(float(total)/float(nbTweets)))
			purity_sum += normalized

		return len(unique_hashtags), len(topicCluster), nbTweets, purity_sum

	def test_iteration(self, collection, num_topics, file):
		dbcollections = MongoClient().twitter[collection]

		tweets = []
		count = 0
		unique_hashtags = set()
		allcount = 0
		for tweet in dbcollections.find({}, {"_id": 1, "entities": 1, "text": 1}):
			allcount += 1
			if not tweet.has_key('text'):
				continue

			hashtags = [] 
			if tweet.has_key('entities') and tweet['entities'].has_key('hashtags')  > 0:
				for i in range(len(tweet['entities']['hashtags'])):
					atag = tweet['entities']['hashtags'][i]['text']
					hashtags.append(atag)
					unique_hashtags.add(atag)

				if len(hashtags) == 0:
					continue

				tweets.append((tweet['text'], tweet['_id'], hashtags))

				count += 1
				if count == 10000:
					break;

	def test_find_max_n(self):
		for collection in ["trends", "sample"]:
			for i in range(8500, 9000, 100):
				hash, nbTopics, nbTweets, purity = semanticModel.test_clustering("sample", i)
				print i, hash, nbTopics, purity	

semanticModel = SemanticClustering()
semanticModel.test_find_max_n()
