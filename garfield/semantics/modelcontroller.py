import subprocess
import time
import requests
import json
import redis
import sys

class ModelManager:

	def __init__(self, hostname, redishost, redisport=6379):
		self.models = []
		
		self.startport = 8000
		self.nextport = 8000
		self.dictsize = 1000
		self.mprefix = "model"
		self.modelcount = 0
		
		self.hostname = hostname

		self.r = redis.StrictRedis(host=redishost, port=redisport)

	def getModelname(self):
		return self.mprefix + str(self.modelcount+1)

	def queryfull(self, modelport):
		url = "http://" + self.hostname +":" + str(modelport) + "/?qdict=isfull"
		try:
			reply = requests.get(url)
			replycontent = json.loads(reply.content)
			bool = replycontent['result']
			return (bool == "True")
		except:
			return False
	
	def spawnModel(self):
		subprocess.Popen(["python", "main.py", str(self.nextport), self.getModelname(), str(self.dictsize)])
		self.models.append( (self.nextport, self.getModelname()))

		self.nextport += 1
		self.modelcount += 1
	
	def publishModelInfo(self):
		modellist = []
		for (modelport, modelname) in self.models:
			modelurl = "http://" + self.hostname + ":" + str(modelport)
			modellist.append({"model": modelname, "url": modelurl})

		msgjson = {"models": modellist}
		self.r.publish("model-info", json.dumps(msgjson))
		
	def run(self):
		self.spawnModel()

		while True:
			time.sleep(5)

			modelport, modelname = self.models[-1:][0]
			isfull = self.queryfull(modelport)

			if isfull:
				print modelname + " is full, spawning new model"
				self.spawnModel()

			self.publishModelInfo()
		

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print "Usage: " + sys.argv[0] + "host redis-host"
		exit(1)

	mg =  ModelManager(sys.argv[1], sys.argv[2])
	mg.run()
