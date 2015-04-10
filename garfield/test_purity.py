#!/usr/bin/python
import sys,os,re,time
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
	
	def applyTransformation(self, corpus, nbTopics, model="LSA"):
		tfidf = models.TfidfModel(corpus)
		corpus_tfidf = tfidf[corpus] 

		docs_topics = []
		if model == "LSA":
			lsi = models.LsiModel(corpus_tfidf, id2word=self.corpusDict, num_topics=nbTopics) # initialize an LSI transformation
			corpus_lsi = lsi[corpus_tfidf] # create a double wrapper over the original corpus: bow->tfidf->fold-in-lsi
			for doc in corpus_lsi: # both bow->tfidf and tfidf->lsi transformations are actually executed here, on the fly
				docs_topics.append(doc)

		if model == "LDA":
			pass

		return docs_topics;
	
	def topicClustering(self, tweets, nbTopics):
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

		docs_topics = self.applyTransformation(corpus, nbTopics)

		topicCluster = {}
		tweetTopic = []
		ty = 0
		for doc in docs_topics:
			max = None
			for atuple in doc:
				if max is None:
					max = atuple
					continue;
				if max[1] < atuple[1]:
					max = atuple
			
			if max is not None:
				max_topic = str(max[0])
				if not topicCluster.has_key(max_topic):
					topicCluster[max_topic] = []

				topicCluster[max_topic].append((max[1], tweets[ty][2],tweets[ty][0]))	

			tweetTopic.append({"tweet": tweets[ty], "max_tuple": max,  "topics_tuple": doc})
			ty += 1

		return topicCluster, tweetTopic

	def calculatePurity(self, topicCluster):

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

		return purity_sum

	def extractTweetsWithHashTag(self, collection):
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
		#print len(unique_hashtags)
		#print len(tweets)
		return tweets, unique_hashtags

	def test_find_max_n(self):
		logfile = open("purity_result.txt", "a")
		collection = "sample"
		tweets, uhashtags = self.extractTweetsWithHashTag(collection)

		stdout = ""
		stdout +=  "--------------------------------\n"
		stdout += "Collection: %s\n"%collection
		stdout += "Tweets : %d \n"%len(tweets)
		stdout += "Unique hastags : %d\n"%len(uhashtags)
		stdout += "--------------------------------\n"
		stdout += "N \t Detected \t Purity \t Time\n" 
		
		print stdout
		logfile.write(stdout)
		for i in range(50, 110, 50):
			t1 = time.time()
			topicCluster,tweetTopic = self.topicClustering(tweets, i)
			t2 = time.time()
			purity = self.calculatePurity(topicCluster)		
			line = "%d \t %d \t %f \t %f\n"%(i, len(topicCluster), purity, round((t2-t1),2))

			logfile.write(line)
			print line

semanticModel = SemanticClustering()
semanticModel.test_find_max_n()
