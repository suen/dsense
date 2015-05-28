import web

urls = (
    '/', 'REST',
)

render = web.template.render('templates')

class REST:        
	def GET(self):
		return "hello world" 
	
	def POST(self):
		pass
		try:
			pass
		except:
			pass
			return None 

if __name__ == "__main__":
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()
