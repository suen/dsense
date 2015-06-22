#!/usr/bin/python

import redis
import json
import time
from threading import Thread

class StreamForwardBot:
	def __init__(self):
		self.redisin = redis.StrictRedis(host="daubajee.com", port=80)
		self.redisout = redis.StrictRedis()

		self.pubsub = self.redisin.pubsub()
		self.pubsub.subscribe("twitter-realtime")

		self.pubsub.get_message()
	
		self.buffer1 = []
		self.buffer2 = []

		thread1 = Thread(target=self.threadrun)
		thread1.setDaemon(True)
		thread1.start()
	

	def threadrun(self):
		count = 0
		while True:
			if len(self.buffer1) == 0 or len(self.buffer2) == 0:
				time.sleep(0.01)
				continue

			if count % 2 == 0:
				msg = self.buffer1[0]
				self.buffer1 = self.buffer1[1:]
			else:
				msg = self.buffer2[0]
				self.buffer2 = self.buffer2[1:]
			
			self.publish(msg)
			
			count += 1
			if count % 200 == 0:
				print "%d published to redis"%count
				


	def publish(self,msg):
		try:
			msg = json.loads(msg['data'])
			self.redisout.publish("twitter-stream", json.dumps(msg))
		
		except:
			print "Json Casting Exception"
		
	def run(self):
		count = 0
		time1 = time.time()
		while True:
			incoming = self.pubsub.get_message()


			if incoming is None:
				time.sleep(0.01)
				continue
			
			if not isinstance(incoming['data'], str):
				print "Data not Text Exception"
				continue
			
			#self.publish(incoming)

			if count % 2 == 0:
				self.buffer1.append(incoming)
			else:
				self.buffer2.append(incoming)

			count += 1

			if count % 200 == 0:
				time2 = time.time()
				delta = time2 - time1
				rate = 200 / delta

				print "%d retrieved, Rate: %f "%(count,rate)

				time1 = time2

if __name__ == "__main__":
	stb = StreamForwardBot()
	stb.run()
