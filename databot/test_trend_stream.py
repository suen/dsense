import http_crawler
import json
import urllib2 as urllib

tc = http_crawler.TwitterAPIClient()	

#for France WOEID
#trendresp = tc.fetchTrends(23424819)

#for the world
trendresp = tc.fetchTrends(1)

trends = []
tr = ""
for r in trendresp:
	try:
		tr = json.loads(r)
	except:
		pass

tr = tr[0]
for trend in tr['trends']:
	try:
		trendstr = str(trend['name'])
		trendstr = trendstr.replace("#", "")
		trends.append(trendstr)
	except:
		pass

trendstr = ",".join(trends)

print "============================================"
print trendstr
print "============================================"

res = tc.fetchPublicStream("en", trendstr)
for r in res:
	print r
tc.saveStream(res, "trends")
