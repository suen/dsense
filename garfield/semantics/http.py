import web
from semantics import LDAModel 
import json

urls = (
    '/', 'REST',
)

class REST:        
	def GET(self):
		param = web.input(query="")
		
		query = param.query
		if query == "":
			return "" 

		model = LDAModel.Instance()
		topics = model.textquery(query)

		result = {"result": topics}

		return json.dumps(result) 
	
	def POST(self):
		self.GET()

def run():
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()

if __name__ == "__main__":
	run()
