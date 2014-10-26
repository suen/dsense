import json
import oauth2 as oauth
import urllib2 as urllib
from pymongo import MongoClient
import sys

##API key HlbiNsr9OqDHRnZCbLq3EraSR
##API secret AK1iPnjxR0eFGlUNtIQisbtuDgbiYohLCiMUBqaZZdElbBrOR1
##Access token 1731483355-QQfk8yxfdvyN2FyYtkzbjLlr1BBM4Myx2US0tU0
##Access token secret LLoDca95B3S0ulC4l5vMM9DFGKxjClyVqYIeuuNUcCFu8

# See assignment1.html instructions or README for how to get these credentials

api_key = "uhcmxEqLoZkhxA8sXoSE2uHvp"
api_secret = "cNzJxBH7a5GkbWXLMHrEXF0iP4VVlEMkFZQEPGt9FsiNgelFA9"
access_token_key ="56294223-s41TrQ3o0vwvmZkR8Sl5Cp7VCe4H5mgVoBXxbLuuj"
access_token_secret = "HeTPrWutZKhkOrCAu8BtUJtc4qTrIjEOVzt63qcuuJrBM" 

_debug = 0

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=api_key, secret=api_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler  = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

'''
Construct, sign, and open a twitter request
using the hard-coded credentials above.
'''
def twitterreq(url, method, parameters):
  req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url, 
                                             parameters=parameters)

  req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

  headers = req.to_header()

  if http_method == "POST":
    encoded_post_data = req.to_postdata()
  else:
    encoded_post_data = None
    url = req.to_url()

  opener = urllib.OpenerDirector()
  opener.add_handler(http_handler)
  opener.add_handler(https_handler)

  response = opener.open(url, encoded_post_data)

  return response

def fetchsamples():
  url = "https://stream.twitter.com/1.1/statuses/filter.json?language=en&track=twitter"
  parameters = []
  response = twitterreq(url, "GET", parameters)
  mclient = MongoClient()
  db = mclient.twitter
  tweets = db.tweets
  c = 0
  for line in response:
    try:
      l = json.loads(line.strip())
      tweets.insert(l)
    except:
      print line
      pass
    c = c + 1

if __name__ == "__main__":
	try:
		fetchsamples()
	except:
		sys.exit(-1)
		pass	

