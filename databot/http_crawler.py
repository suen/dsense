import json
import oauth2 as oauth
import urllib2 as urllib
from pymongo import MongoClient
import sys,traceback
import time
import ConfigParser
from threading import Thread

class TwitterAPIClient:

	def __init__(self):
	
		Config = ConfigParser.ConfigParser()

		Config.read("api_token.ini")

		self.api_key = Config.get("TwitterApiToken", "api_key") 
		self.api_secret = Config.get("TwitterApiToken", "api_secret") 
		self.access_token_key = Config.get("TwitterApiToken", "access_token_key") 
		self.access_token_secret = Config.get("TwitterApiToken", "access_token_secret") 

		self._debug = 0

		self.oauth_token    = oauth.Token(key=self.access_token_key, secret=self.access_token_secret)
		self.oauth_consumer = oauth.Consumer(key=self.api_key, secret=self.api_secret)

		self.signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

		self.http_method = "GET"

		self.http_handler  = urllib.HTTPHandler(debuglevel=self._debug)
		self.https_handler = urllib.HTTPSHandler(debuglevel=self._debug)

	'''
	Construct, sign, and open a twitter request
	using the hard-coded credentials above.
	'''
	def twitterRequest(self, url, method, parameters=[]):
	  self.req = oauth.Request.from_consumer_and_token(self.oauth_consumer,
												 token=self.oauth_token,
												 http_method=method,
												 http_url=url, 
												 parameters=parameters)

	  self.req.sign_request(self.signature_method_hmac_sha1, self.oauth_consumer, self.oauth_token)

	  headers = self.req.to_header()

	  if method == "POST":
		encoded_post_data = self.req.to_postdata()
	  else:
		encoded_post_data = None
		url = self.req.to_url()

	  opener = urllib.OpenerDirector()
	  opener.add_handler(self.http_handler)
	  opener.add_handler(self.https_handler)

	  response = opener.open(url, encoded_post_data)

	  return response

	def search(self, keyword):
		url = "https://api.twitter.com/1.1/search/tweets.json?q="+ urllib.quote(keyword)+"&count=3000&lang=en"
		response = self.twitterRequest(url, "GET");
		return response

	def fetchUserTimeline(self, screenHandle):
		url = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="+screenHandle+"&count=3000"
		response = self.twitterRequest(url, "GET");
		return response

	def fetchPublicStream(self, lang="en", track="twitter"):
		url = "https://stream.twitter.com/1.1/statuses/filter.json?language="+lang+"&track="+urllib.quote(track)
		response = self.twitterRequest(url, "POST");
		return response
	
	def fetchPublicStreamSample(self):
		url = "https://stream.twitter.com/1.1/statuses/sample.json"
		response = self.twitterRequest(url, "GET");
		return response
	
	def saveFilterStream(self, urlresponse, collection, minNbWords=1, lang="en"):
		mclient = MongoClient()
		dbcollection = mclient.twitter[collection]
		self.buffer = []
		
		t = Thread(target=self.saveFilterStreamThread, args=(dbcollection,minNbWords,lang) )
		t.setDaemon(True)
		t.start()

		count = 0
		time1 = time.time()
		for r in urlresponse:
			self.buffer.append(r)
			count += 1
			if count % 100 == 0:
				time2 = time.time()
				delta = time2 - time1
				rate = 100 / delta
				print "Incoming count: %d, Rate: %f"%(count,rate)
				time1 = time2

	
	def saveFilterStreamThread(self, dbcollection, minNbWords, lang):
		processed = 0
		reject = 0
		time1 = time.time()
		while True:
			if len(self.buffer) == 0:
				time.sleep(0.00001)
				continue

			tweet = self.buffer.pop()
			try:
				r = json.loads(tweet.strip())
				
				if (r['lang'] == lang and len(r['text'].strip(" ")) >= minNbWords):
					#print r['text'] 
					rs = {"text": r['text'], "id": r['id'], "created_at": r['created_at'], "user": r['user']['screen_name']}
					dbcollection.insert(rs)
					processed += 1

				else:
					reject += 1
			except:
				reject += 1
				pass

			if (processed+reject) % 100 == 0:
				time2 = time.time()
				delta = time2 - time1
				rate = 100 / delta
				print "Processed : %d + Reject: %d = %d, Rate: %f"%(processed,reject,processed+reject,rate)
				time1 = time2
	
	def fetchTrends(self, woeid=1):
		url = "https://api.twitter.com/1.1/trends/place.json?id="+str(woeid)
		response = self.twitterRequest(url, "GET");
		return response

	def saveStream(self, urlresponse, collection):
		mclient = MongoClient()
		dbcollection = mclient.twitter[collection]
		for r in urlresponse:
			try:
				r = json.loads(r.strip())
				dbcollection.insert(r)
			except:
				pass
	
	def printStream(self, response, projection=[]):
		for r in response:
			try:
				r = json.loads(r.strip())
				if r['lang'] == "en":
					print r['text']
			except:
				pass
		
	def getTweetsFromUsers(self, accounts=[]):
		mclient = MongoClient()
		db = mclient.twitter
		userdb = db.usersTmp

		if len(accounts) == 0:
			accounts = ["barackobama", "arstechnica", "beINSPORTS", "vevo", "cosmopolitan"]

		for acc in accounts:
			result = self.fetchUserTimeline(acc)

			count = 0
			#f = open("tweets.txt", "w")
			for l in result:
				l = json.loads(l.strip())
				userdb.insert(l)
				#f.write(l)
				count += 1;
				#print l
			print "%s %d fetched"%(acc,count)

if __name__ == "__main__":
	tc = TwitterAPIClient()	
	resp = tc.fetchPublicStreamSample()
	tc.saveFilterStream(resp, "save", 5)
