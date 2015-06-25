import redis
import time
from threading import Thread
import json
from patterns import Singleton
import random

class FeedListener:
	def __init__(self, redishost):
		self.r = redis.StrictRedis(host=redishost)
		self.p = self.r.pubsub()
		self.p.subscribe("twitter-stream")

		self.p2 = self.r.pubsub()
		self.p2.subscribe("topwords")

		self.handler = None
		
		print "====", self.p.get_message()
		print "====", self.p2.get_message()

		self.dataThread = Thread(target=self.listen)
		self.dataThread.setDaemon(True)

		self.topwordThread = Thread(target=self.listenTopWords)
		self.topwordThread.setDaemon(True)

	def setDataHandler(self, handler):
		self.handler = handler
	
	def startListen(self):
		self.dataThread.start()
		self.topwordThread.start()
	
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

	def listenTopWords(self):
		while True:
			msg = self.p2.get_message()
			if msg == None:
				time.sleep(1)
				continue
			if not isinstance(msg['data'],str): 
				print type(msg['data'])
				print msg
				continue
			if self.handler is not None:
				try:
					msg = json.loads(msg['data'].replace("\n", ""))
					self.handler.feedTopWords(msg)
				except:
					print msg
					print "JSON casting exception"
			else:
				print msg['data'] 

@Singleton
class StreamFixedQueue:
	def init(self, redishost):
		self.listener = FeedListener(redishost)
		self.listener.setDataHandler(self)
		self.buffer = []
		self.size = 10

		self.topwords = []
		self.tsize = 20
		self.counter = 0
		self.lastcounter = 0
		self.time1 = time.time()
		self.starttime = time.time()
		return self

	def start(self):
		self.listener.startListen()
	
	def feed(self,msg):
		self.buffer.append(msg)
		if len(self.buffer) > self.size: 
			self.buffer = self.buffer[1:]
		self.counter += 1
	
	def getTweetRate(self):
		time2 = time.time()
		rate = (self.counter - self.lastcounter) / (time2 - self.time1)
		self.lastcounter = self.counter 
		self.time1 = time2
		uptime = self.time1 - self.starttime
		return self.counter, round(rate,2), uptime

	
	def feedTopWords(self, word):
		self.topwords.append(word)
		if len(self.topwords) > self.tsize:
			self.topwords = self.topwords[1:]

	def getTopwords(self):
		return self.topwords
	
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
