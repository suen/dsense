#!/usr/bin/python

import redis
import json
import time

redisin = redis.StrictRedis(host="daubajee.com", port=80)
redisout = redis.StrictRedis()

pubsub = redisin.pubsub()
pubsub.subscribe("twitter-realtime")

pubsub.get_message()

count = 0
time1 = time.time()
while True:
	incoming = pubsub.get_message()


	if incoming is None:
		time.sleep(0.01)
		continue
	
	if not isinstance(incoming['data'], str):
		print "Data not Text Exception"
		time.sleep(0.01)
		continue
	
	try:
		msg = json.loads(incoming['data'])
		redisout.publish("twitter-stream", json.dumps(msg))
	
	except:
		print "Json Casting Exception"

	count += 1

	if count % 200 == 0:
		time2 = time.time()
		delta = time2 - time1
		rate = 200 / delta

		print "%d forwarded, Rate: %f "%(count,rate)

		time1 = time2


