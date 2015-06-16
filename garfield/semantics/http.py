import web
from semantics import ModelDict 
import json

urls = (
    '/', 'REST',
	'/feedback', 'Feedback',
)

class REST:        
	def GET(self):
		param = web.input(query="", getTopics="", getDictionary="")
		
		query = param.query
		topics = param.getTopics
		dictionary = param.getDictionary
		if query != "":
			model = ModelDict.Instance().model
			topics = model.textquery(query)

			result = {"result": topics}

			return json.dumps(result) 

		if topics != "":
			model = ModelDict.Instance().model
			topiclist = model.show_topics(int(topics)) 
			
			result = {"result": topiclist}

			return json.dumps(result) 

		if dictionary != "":
			map = ModelDict.Instance().dict.token2id
			result = {"result": map}

			return json.dumps(result) 


		return "" 
	
	def POST(self):
		self.GET()
	
class Feedback:
	def GET(self):
		param = web.input(word="")

		word = param.word
		if word != "":
			words = [ x.strip() for x in open("top1000.txt").readlines() ]
			for w in words:
				Main.Instance().wdict.add_word(w)
			return "OK"

			response = Main.Instance().wdict.add_word(word)
			if response:
				return "OK"
			else:
				return "ADD_ERROR"
		
		return "";
	
	def POST(self):
		return self.GET()

def run():
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()

if __name__ == "__main__":
	run()
