import subprocess
import time
import requests
import json

class ModelManager:

	def __init__(self):
		self.models = []
		
		self.startport = 8000
		self.nextport = 8000
		self.dictsize = 1000
		self.mprefix = "model"
		self.modelcount = 0

	def getModelname(self):
		return self.mprefix + str(self.modelcount+1)

	def queryfull(self, modelport):
		url = "http://localhost:" + str(modelport) + "/?qdict=isfull"
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
		
	def run(self):
		#run the first model
		self.spawnModel()

		while True:
			time.sleep(5)

			modelport, modelname = self.models[-1:][0]
			isfull = self.queryfull(modelport)

			if isfull:
				print modelname + " is full, spawning new model"
				self.spawnModel()
			 

		

if __name__ == "__main__":
	mg =  ModelManager()
	mg.run()
	
	
