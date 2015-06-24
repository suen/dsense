import requests
import json
import time
import redis
from threading import Thread
from patterns import Singleton
import pymongo 

class HClient:
	def __init__(self, serveraddress):
		self.serveraddress = serveraddress
	
	def query(self, keywords):
		url = self.serveraddress + "/?query=" + keywords
		reply = requests.get(url)
		replycontent = json.loads(reply.content)
		return replycontent

@Singleton
class ModelAccess:
	def init(self, redishost, mongodbhost, redisport=6379):
		self.mongodbhost = mongodbhost
		self.r = redis.StrictRedis(host=redishost, port=redisport)
		self.p = self.r.pubsub()
		self.p.subscribe("model-info")
		self.p.get_message()

		self.thread = Thread(target=self.listenModelInfo)
		self.thread.setDaemon(True)
		self.thread.start()

		self.models = []
		self.mgwarehouse = pymongo.MongoClient(host=mongodbhost).twitter.warehouse

	def update(self, msgjson):
		self.models = msgjson['models'] 
		print "Model list updated"

	def queryModelInfo(self, address):
		url = address + "/?qdict=gettokens"
		reply = requests.get(url)
		replycontent = json.loads(reply.content)
		return replycontent

	def modelinfoFormat(self, modelinfo):
		index = modelinfo['nextindex']
		tokensID = modelinfo['tokensID']
		
		for i in range(index, 1000):
			if tokensID.has_key("abcdefgh" + str(i)):
				del tokensID["abcdefgh" + str(i)]
		return {"words": tokensID, "count": index}


	def modelinfo(self):
		modelinfolist = []
		for modeldict in self.models:
			modeladdress = modeldict['url']
			modelinfo = self.queryModelInfo(modeladdress)

			modelinfo = self.modelinfoFormat(modelinfo)

			modelinfo['name'] = modeldict['model']
			modelinfolist.append(modelinfo)

		return modelinfolist


	def querymodel(self, address, keywords):
		url = address + "/?query=" + keywords
		reply = requests.get(url)
		replycontent = json.loads(reply.content)
		return replycontent

	def query(self, keywords):
		resultModels = []

		for modeldict in self.models:
			modeladdress = modeldict['url']

			modelreply = self.querymodel(modeladdress, keywords)

			resultModels += modelreply['result']

		print resultModels
		
		tweets = set() 
		for result in resultModels:
			model = result['model']
			cursor = self.mgwarehouse.find({"topic": {"$elemMatch": { "model": model, "score": {"$gte" : 0.10 }} }}, {"topic.$":1, "text": 1}).sort("topic.score", pymongo.DESCENDING).limit(30)
			#cursor = mgclient.find({"topic.model": model}, {"text":1, "topic": 1})
			for d in cursor:
				doc = d['text']
				#score = d['topic'][model]
				tweets.add(doc)

		return tweets 


	
	def listenModelInfo(self):
		while True:
			msg = self.p.get_message()
			if msg == None:
				time.sleep(1)
				continue
			if not isinstance(msg['data'],str): 
				print type(msg['data'])
				print msg
				continue
			msg = json.loads(str(msg['data']))
			self.update(msg)

if __name__ == "__main__":
	hclient = HClient("http://localhost:8080")
	print hclient.query("death")

