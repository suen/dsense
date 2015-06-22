import redis
import json
import time
from http_crawler import TwitterAPIClient
from threading import Thread


class PublishToRedis:
	def __init__(self):
		self.redisclient = redis.StrictRedis()
		self.tc = TwitterAPIClient()	

		self.buffer2 = []
		self.buffer1 = []

		self.pthread = Thread(target=self.thread1)
		self.pthread.setDaemon(True)
		self.pthread.start()

		self.rthread = Thread(target=self.thread2)
		self.rthread.setDaemon(True)
		self.rthread.start()
	
	def run(self):
		time1 = time.time()

		count = 0
		while True:
			resp = self.tc.fetchPublicStreamSample()
			for r in resp:
				
				
				if count % 2 == 0:
					self.buffer1.append(r)
				else:
					self.buffer2.append(r)

				count += 1
				if count % 500 == 0:
					time2 = time.time()
					delta = time2 - time1
					rate = 500 / delta
					print "%d tweets published, Rate: %f tweets per second"%(count,rate)
					time1 = time2
			
			print "Stream failed, retrying in 10 seconds"
			time.sleep(10) 

	def thread1(self):
		count = 0
		while True:
			if len(self.buffer1) == 0:
				time.sleep(0.1)
				continue

			msg = self.buffer1[0]
			self.buffer1 = self.buffer1[1:]

			self.process(msg)
			count += 1

			if count % 500 == 0:
				print "Thread1: %d processed"%count

	def thread2(self):
		count = 0
		while True:
			if len(self.buffer2) == 0:
				time.sleep(0.1)
				continue

			msg = self.buffer2[0]
			self.buffer2 = self.buffer2[1:]
			
			self.process(msg)

			count += 1

			if count % 500 == 0:
				print "Thread2: %d processed"%count


	def process(self, msg):
		msg = json.loads(msg)
		if not msg.has_key('lang') or msg['lang'] != "en":
			return	

		text = msg['text']
		if len(text.split()) <= 4:
			return	
		
		plaintweet = {"text": text, "id": msg['id'] }

		self.redisclient.publish("twitter-realtime", json.dumps(plaintweet))

	
if __name__ == "__main__":
	ptr = PublishToRedis()	
	ptr.run()
