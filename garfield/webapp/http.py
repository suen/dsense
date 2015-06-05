import web
import json
from hclient import HClient
from pymongo import MongoClient

urls = (
    '/', 'Static',
    '/rest', 'REST',
)

render = web.template.render("static")

class Static:
	def GET(self):
		return render.index()
	
	def POST(self):
		pass

class REST:        
	def GET(self):
		httpclient = HClient("http://localhost:8080")
		mgclient = MongoClient().twitter.warehouse
		param = web.input(query="")
		
		query = param.query
		if query == "":
			return "" 
			
		print "Query = " + query

		resultquery = httpclient.query(query)

		#this is a list
		querytopics = resultquery['result']
		print querytopics

		tweets = []
		for result in resultquery['result']:
			model = result['model']
			
			cursor = mgclient.find({"topic.model": model}, {"text":1})
			for d in cursor:
				doc = d['text']
				print doc
				tweets.append(doc)

		print tweets

		results = {"results": ["This is tweet1", "Tweet 2 is here",
							"Tweet 3 is here", "There is sth here" ] }

		results = {"results": tweets}

		return json.dumps(results) 

		model = LDAModel.Instance()
		topics = model.textquery(query)

		return str(topics) 
	
	def POST(self):
		self.GET()

def run():
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()

if __name__ == "__main__":
	run()
