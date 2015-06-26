import web
import json
from model import ModelAccess 
from pymongo import MongoClient
from realtime import StreamFixedQueue
import pymongo
import sys

urls = (
    '/', 'Static',
	'/admin', 'AdminStatic',
    '/rest', 'REST',
    '/realtime', 'Realtime',
)

render = web.template.render("static")

class Static:
	def GET(self):
		return render.index()
	
	def POST(self):
		pass

class AdminStatic:
	def GET(self):
		return render.admin()

class Realtime:

	def GET(self):
		stream = StreamFixedQueue.Instance()

		counter,rate,uptime = stream.getTweetRate()
		
		results = {"results": stream.getSample(), "topwords": stream.getTopwords(), "counter": counter, "rate":rate, "uptime": uptime }
		return json.dumps(results)
	
	def POST(self):
		return self.GET()	

class REST:        
	def GET(self):
		#httpclient = HClient("http://localhost:8080")
		#mgclient = MongoClient().twitter.warehouse
		param = web.input(query="", modelinfo="")
		
		query = param.query
		modelinfo = param.modelinfo
		if query == "" and modelinfo == "":
			return "" 

		if modelinfo != "":
			modellist = ModelAccess.Instance().modelinfo()
			result = {"results": modellist}
			print result
			return json.dumps(result) 

			
		print "Query = " + query

		'''
		#resultquery = httpclient.query(query)
		#resultquery = ModelAccess.Instance().query(query) 

		#this is a list
		#print "Result Models: ",resultquery 

		tweets = set() 
		for result in resultquery:
			model = result['model']
			cursor = mgclient.find({"topic": {"$elemMatch": { "model": model, "score": {"$gte" : 0.10 }} }}, {"topic.$":1, "text": 1}).sort("topic.score", pymongo.DESCENDING).limit(30)
			#cursor = mgclient.find({"topic.model": model}, {"text":1, "topic": 1})
			for d in cursor:
				doc = d['text']
				#score = d['topic'][model]
				tweets.add(doc)

		print tweets
		'''
		tweets = ModelAccess.Instance().query(query) 
		results = {"results": list(tweets)}

		return json.dumps(results) 

		model = LDAModel.Instance()
		topics = model.textquery(query)

		return str(topics) 
	
	def POST(self):
		self.GET()

def run(redishost, mongodbhost):
	StreamFixedQueue.Instance().init(redishost).start()
	ModelAccess.Instance().init(redishost, mongodbhost)
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()

if __name__ == "__main__":
	if len(sys.argv) != 4:
		print "Usage: " + sys.argv[0] + " <HTTP PORT> <REDIS HOST> <MONGODB HOST>"
		exit(1)
	run(sys.argv[2], sys.argv[3])
