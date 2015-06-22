import redis
import json
import time
from http_crawler import TwitterAPIClient
from threading import Thread



def processthread():
	while True:
		if len(buffer) == 0:
			time.sleep(1)
			continue

		msg = buffer[0]
		buffer = buffer[1:]

		msg = json.loads(msg)
		if not msg.has_key('lang') or msg['lang'] != "en":
			continue

		text = msg['text']
		if len(text.split()) <= 4:
			continue
		
		plaintweet = {"text": text, "id": msg['id'] }

		redisclient.publish("twitter-realtime", json.dumps(plaintweet))
	

redisclient = redis.StrictRedis()
tc = TwitterAPIClient()	

count = 0
buffer = []
time1 = time.time()
while True:
	resp = tc.fetchPublicStreamSample()
	for r in resp:
		
		self.buffer.append(r)

		count += 1
		if count % 500 == 0:
			time2 = time.time()
			delta = time2 - time1
			rate = 500 / delta
			print "%d tweets published, Rate: %f tweets per second"%(count,rate)
			time1 = time2
	
	print "Stream failed, retrying in 10 seconds"
	time.sleep(10)
