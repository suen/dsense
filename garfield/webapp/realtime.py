import redis
import time
from threading import Thread
import json
from patterns import Singleton
import random

class FeedListener:
	def __init__(self):
		self.r = redis.StrictRedis()
		self.p = self.r.pubsub()
		self.p.subscribe("twitter-persist")
		self.handler = None
		
		print "====", self.p.get_message()

		self.dataThread = Thread(target=self.listen)
		self.dataThread.setDaemon(True)

	def setDataHandler(self, handler):
		self.handler = handler

	def startListen(self):
		self.dataThread.start()
	
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

@Singleton
class StreamFixedQueue:
	def __init__(self):
		self.listener = FeedListener()
		self.listener.setDataHandler(self)
		self.buffer = []
		self.size = 10

	def start(self):
		self.listener.startListen()
	
	def feed(self,msg):
		self.buffer.append(msg)
		if len(self.buffer) > self.size: 
			self.buffer = self.buffer[1:]
	
	def getSample(self):
		return self.buffer
		'''
		max = len(self.buffer)
		if max <= 10:
			return self.buffer
		else :
			rindex = random.randint(0, max-10)
			return self.buffer[rindex:rindex+10]
		'''
	
	
if __name__ == "__main__":
	stream = StreamFixedQueue.Instance()
	stream.start()

	while True:
		time.sleep(1)

		sample = stream.getSample()

		print "-----"
		print sample
