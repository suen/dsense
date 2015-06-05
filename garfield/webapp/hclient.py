import requests
import json

class HClient:
	def __init__(self, serveraddress):
		self.serveraddress = serveraddress
	
	def query(self, keywords):
		url = self.serveraddress + "/?query=" + keywords
		reply = requests.get(url)
		replycontent = json.loads(reply.content)
		return replycontent

if __name__ == "__main__":
	hclient = HClient("http://localhost:8080")
	print hclient.query("death")

