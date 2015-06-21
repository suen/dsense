import requests
import json
import time
import redis
from threading import Thread
from patterns import Singleton

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
	def init(self, redishost, redisport=6379):
		self.r = redis.StrictRedis(host=redishost, port=redisport)
		self.p = self.r.pubsub()
		self.p.subscribe("model-info")
		self.p.get_message()

		self.thread = Thread(target=self.listenModelInfo)
		self.thread.setDaemon(True)
		self.thread.start()

		self.models = []

	def update(self, msgjson):
		self.models = msgjson['models'] 
		print "Models updated"

	def queryModel(self, address, keywords):
		url = address + "/?query=" + keywords
		reply = requests.get(url)
		replycontent = json.loads(reply.content)
		return replycontent

	def query(self, keywords):
		resultModels = []

		for modeldict in self.models:
			modeladdress = modeldict['url']

			modelreply = self.queryModel(modeladdress, keywords)

			resultModels += modelreply['result']

		return resultModels


	
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

