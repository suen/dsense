from pymongo import MongoClient
from pattern.vector import Document, Model, KMEANS, RANDOM, COSINE, EUCLIDEAN, MANHATTAN, RANDOM, KMPP
import random


class Cluster:
	
	def __init__(self):
		self.mg = MongoClient()
		self.db = self.mg.twitter
	
	'''
	retrieve n tweet from the database and return it as
	a list of string
	'''
	def	retrieveTweets(self, tweetCount, wordThreshold=3, aleatoire=False):
		if aleatoire:
			twc = self.db.tweetText.find().count()
			pivot = random.randint(tweetCount, twc - tweetCount - 1)
			thalf = int(tweetCount/2)
			c = self.db.tweetText.find().skip(pivot - thalf).limit(tweetCount)
		else:
			c = self.db.tweetText.find().limit(tweetCount)
		tt = []
		for t in c:
			words = t['histo']
			if len(words) < wordThreshold:
				continue
			sent = ""
			for word in words:
				sent += word + " "
			tt.append(sent.strip())
		return tt

	'''
	transforms a string list into pattern.vector.Document list
	'''
	def toDocList(self, strList):
		ttDoc = []
		for str in strList:
			ttDoc.append(Document(str))
		return ttDoc

	
	def kmeansCluster(self, documentList, k, iteration, distance, seed, p):
		if distance.lower() == "cosine":
			distance = COSINE
		elif distance.lower() == "euclidean":
			distance = EUCLIDEAN
		elif distance.lower() == "manhattan":
			distance = MANHATTAN
		else:
			return "invalid distance"

		if seed.lower() == "kmpp":
			seed = KMPP
		elif seed.lower() =="random":
			seed = RANDOM
		else:
			return "invalid random"
		
		if type(k) is not int:
			return "k is not int"

		if type(iteration) is not int:
			return "iterartion is not int"

		if type(p) is not float and type(p) is not int:
			return "p is not float"

		if type(documentList) is not list:
			return "document List is not list"

		self.iteration = iteration
		self.seed = seed
		self.p = p
		self.distance = distance
		
		model = Model(documentList)
		cluster = model.cluster(method=KMEANS, k=k, iterations=iteration, distance=distance,seed=seed,p=p)
		return cluster
		
	'''
	extracts the original text from the pattern.vector.Document
	'''
	def docStr(self, doc):
		s = ""
		for w in doc.features:
			s += w + " "
		s = s.strip()
		return s
	
	'''
	transforms a pattern.vector.Document inside a list into list of strings
	'''
	def textualise(self, list):
		strList = [] 
		for group in list:
			grpstr = [] 
			for doc in group:
				grpstr.append(self.docStr(doc)) 
			strList.append(grpstr)
			
		return strList
	
	'''
	returns n number of random text from the text list
	'''
	def pickRandom(self, list, number):
		indices = []
		randomText = []
		while len(indices) < number:
			aleatoire = random.randint(0, len(list)-1) 
			if aleatoire not in indices:
				indices.append(aleatoire)
				randomText.append(list[aleatoire])
		return randomText
	
	def sampleCluster(self, bigCluster, nbText):
		clusterTrimmed = []
		for acluster in bigCluster:
			souscluster = self.kmeansCluster(acluster, nbText, self.iteration, self.distance, self.seed, self.p)
			
			short = []
			for bcluster in souscluster:
				short += self.pickRandom(bcluster,1)
			clusterTrimmed.append(short)
		
		return self.textualise(clusterTrimmed)
		

if __name__ == "__main__":
	
	clustering = Cluster()
	docList = clustering.toDocList(clustering.retrieveTweets(1000,4,True))
	print "clustering .."
	cluster = clustering.kmeansCluster(docList, 10, 10, "cosine", "random", 0.5)
	print "size of clusters : " + str(len(cluster))

	print str(clustering.sampleCluster(cluster, 2)).replace("u", "")
		
