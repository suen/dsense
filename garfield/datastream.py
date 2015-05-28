import redis
import time
from threading import Thread

class FeedListener:

	def __init__(self):
		r = redis.StrictRedis()
		self.p = r.pubsub()
		self.p.subscribe("twitter-feed")
		
		self.dataThread = Thread(target=self.listen)
		self.dataThread.setDaemon(True)

	def setDataHandler(self, handler):
		self.handler = handler

	def startListen(self):
		self.dataThread.start()

	def listen(self):
		print "here"
		while True:
			msg = self.p.get_message()
			if msg == None:
				time.sleep(1)
				continue
			print msg

class TwitterStream:
	def __init__(self):
		words = [ x.strip() for x in open("dictnostops.txt").readlines() ]
		self.dictionary = corpora.Dictionary()
		self.dictionary.add_documents([words])
	
	def feed(self, data)
	
	def query(self, collection, start, count):
		self.collection = collection
		self.start = start
		self.count = count
		self.initCursor()

	def initCursor(self):
		self.db = MongoClient().twitter[self.collection]
		self.cursor = self.db.find({"text": {"$exists": True}},{"text":1}).skip(self.start).limit(self.count)
	
	def clean(self, text):
		text = text.replace("RT", "")
		
		hi = text.find("http")
		if hi != -1 :
			text += " "
			si = text.find(" ",hi)
			text = text[0:hi]+text[si:]
			text = text.strip() 
		return text

	def __iter__(self):
		self.initCursor()
		for d in self.cursor:
			yield self.dictionary.doc2bow(self.clean(d['text']).lower().split())
	
if __name__ == "__main__":
	
	d = FeedListener()
	d.startListen()
	
	d.dataThread.join()
