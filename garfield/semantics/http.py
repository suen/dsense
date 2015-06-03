import web
from semantics import LDAModel 

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

		return str(topics) 
	
	def POST(self):
		self.GET()

def run():
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()

if __name__ == "__main__":
	run()
