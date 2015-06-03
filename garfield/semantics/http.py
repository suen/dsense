import web

urls = (
    '/', 'REST',
)

model = None 

class REST:        
	def GET(self):
		if model is None:
			return "Hello world"

		return str(model) 
	
	def POST(self):
		pass
		try:
			pass
		except:
			pass
			return None 

def run():
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()

if __name__ == "__main__":
	run()
