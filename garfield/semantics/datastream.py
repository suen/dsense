import redis
import time
from threading import Thread
import json

class FeedListener:

	def __init__(self):
		self.r = redis.StrictRedis()
		self.p = self.r.pubsub()
		self.p.subscribe("twitter-feed")
		self.handler = None
		
		print "====", self.p.get_message()

		self.dataThread = Thread(target=self.listen)
		self.dataThread.setDaemon(True)

	def setDataHandler(self, handler):
		self.handler = handler

	def startListen(self):
		self.dataThread.start()
	
	def publish(self, msg):
		if isinstance(msg, list):
			for m in msg:
				self.r.publish("twitter-persist", json.dumps(m) )
			msgsize = len(msg)
			print "INFO: " + str(msgsize) + " tweets published"
		else:
			self.r.publish("twitter-persist", json.dumps(msg) )
			print "INFO: 1 tweet published"
		

	def listen(self):
		while True:
			msg = self.p.get_message()
			if msg == None:
				time.sleep(1)
				continue
			if not isinstance(msg['data'],str): 
				print type(msg['data'])
				print msg
				continue
			if self.handler is not None:
				msg = json.loads(str(msg['data']))
				self.handler.feed(msg)
			else:
				print msg['data'] 

class HandlerTest:
	def __init__(self):
		pass
	
	def feed(self,msg):
		print len(str(msg)), " chars feed"	
	
if __name__ == "__main__":
	h = HandlerTest()
	d = FeedListener()
	d.setDataHandler(h)
	d.startListen()
	
	d.dataThread.join()
