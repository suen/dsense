import web
import json
from hclient import ModelAccess 
from pymongo import MongoClient
from realtime import StreamFixedQueue
import pymongo

urls = (
    '/', 'Static',
    '/rest', 'REST',
    '/realtime', 'Realtime',
)

render = web.template.render("static")

class Static:
	def GET(self):
		return render.index()
	
	def POST(self):
		pass

class Realtime:

	def GET(self):
		stream = StreamFixedQueue.Instance()
		
		results = {"results": stream.getSample(), "topwords": stream.getTopwords()}
		return json.dumps(results)
	
	def POST(self):
		return self.GET()	

class REST:        
	def GET(self):
		#httpclient = HClient("http://localhost:8080")
		mgclient = MongoClient().twitter.warehouse
		param = web.input(query="")
		
		query = param.query
		if query == "":
			return "" 
			
		print "Query = " + query

		#resultquery = httpclient.query(query)
		resultquery = ModelAccess.Instance().query(query) 

		#this is a list
		print "Result Models: ",resultquery 

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

		results = {"results": ["This is tweet1", "Tweet 2 is here",
							"Tweet 3 is here", "There is sth here" ] }

		results = {"results": list(tweets)}

		return json.dumps(results) 

		model = LDAModel.Instance()
		topics = model.textquery(query)

		return str(topics) 
	
	def POST(self):
		self.GET()

def run():
	StreamFixedQueue.Instance().start()
	ModelAccess.Instance().init("127.0.0.1")
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()

if __name__ == "__main__":
	run()
