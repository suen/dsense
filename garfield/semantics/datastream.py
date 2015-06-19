import redis
import time
from threading import Thread
import json

class FeedListener:

	def __init__(self):
		self.r = redis.StrictRedis()
		self.streamchannel = self.r.pubsub()
		self.streamchannel.subscribe("twitter-stream")

		self.feedbackchannel = self.r.pubsub()
		self.feedbackchannel.subscribe("topwords")

		self.handler = None
		
		print "====", self.streamchannel.get_message()
		print "====", self.feedbackchannel.get_message()

		self.streamThread = Thread(target=self.listenStream)
		self.streamThread.setDaemon(True)

		self.feedbackThread = Thread(target=self.listenFeedback)
		self.feedbackThread.setDaemon(True)

	def setStreamHandler(self, handler):
		self.streamHandler = handler

	def setFeedbackHandler(self, handler):
		self.feedbackHandler = handler

	def startListen(self):
		self.streamThread.start()
		self.feedbackThread.start()
	
	def publish(self, msg):
		if isinstance(msg, list):
			for m in msg:
				self.r.publish("twitter-postmodel", json.dumps(m) )
			msgsize = len(msg)
			#print "INFO: " + str(msgsize) + " tweets published"
		else:
			self.r.publish("twitter-postmodel", json.dumps(msg) )
			#print "INFO: 1 tweet published"
		

	def listenStream(self):
		while True:
			msg = self.streamchannel.get_message()
			if msg == None:
				time.sleep(0.1)
				continue
			if not isinstance(msg['data'],str): 
				print type(msg['data'])
				print msg
				continue
			if self.streamHandler is not None:
				msg = json.loads(str(msg['data']))
				self.streamHandler.feed(msg)
			else:
				print msg['data'] 

	def listenFeedback(self):
		while True:
			msg = self.feedbackchannel.get_message()
			if msg == None:
				time.sleep(0.1)
				continue
			if not isinstance(msg['data'],str): 
				print type(msg['data'])
				print msg
				continue
			if self.feedbackHandler is not None:
				try:
					msg = json.loads(str(msg['data']))
					self.feedbackHandler.feedback(msg)
				except:
					print "Exception"
					print msg['data']
			else:
				print msg['data'] 

class HandlerTest:
	def __init__(self):
		pass
	
	def feed(self,msg):
		print len(str(msg)), " chars feed"	
	
	def feedback(self, msg):
		print "Feedback : " + str(msg)
	
if __name__ == "__main__":
	h = HandlerTest()
	d = FeedListener()
	d.setStreamHandler(h)
	d.setFeedbackHandler(h)
	d.startListen()
	
	d.feedbackThread.join()
	d.streamThread.join()
