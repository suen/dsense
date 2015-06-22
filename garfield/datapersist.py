import redis
from threading import Thread
from pymongo import MongoClient
import json, time, sys

class FeedPersistence:
	
	def __init__(self, redishost, redisport=6379):
		self.mg = MongoClient().twitter.warehouse
		self.r = redis.StrictRedis(host=redishost, port=redisport)
		self.p = self.r.pubsub()
		self.p.subscribe("twitter-postmodel")

		print self.p.get_message()
		self.handler = None

		self.listenThread = Thread(target=self.listen)
		self.listenThread.setDaemon(True)

		self.persistThread = Thread(target=self.persist)
		self.persistThread.setDaemon(True)

		self.buffer = []
		self.packet = []
		self.packetsize = 10

	def start(self):
		self.listenThread.start()
		self.persistThread.start()
	
	def persist(self):
		written = 0
		while True:
			if len(self.buffer) > 1:
				packet = self.buffer[0]
				for p in packet:
					self.mg.insert(p)
				written += len(packet)
				print str(written) + " written"
				self.buffer = self.buffer[1:]
			else:
				time.sleep(1)

	def listen(self):
		while True:
			msg = self.p.get_message()
			if msg == None:
				time.sleep(1)
				continue
			if not isinstance(msg['data'],str): 
				#print type(msg['data'])
				#print msg
				continue
			msg = json.loads(str(msg['data']))

			self.packet.append(msg)

			if len(self.packet) > self.packetsize: 
				self.buffer.append(self.packet)
				self.packet = []

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print "Usage: " + sys.argv[0] + " <Redis host>"
		exit(1)

	dt = FeedPersistence(sys.argv[1])
	dt.start()
	dt.listenThread.join()
