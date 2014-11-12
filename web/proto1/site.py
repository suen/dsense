import web
from subprocess import call
import os, sys, traceback
from DsenseClustering import Cluster

urls = (
    '/', 'index',
    '/result', 'result',
)

render = web.template.render('templates')

class index:        
	def GET(self):
		return render.index({},[],"")
	
	def POST(self):
		x = web.input()

		try:
			distance = x.distance
			seed = x.seed
			k = int(x.k)
			p = float(x.p)
			iteration = int(x.iteration)
			nbTweets = int(x.nbtweets)
			minWord = int(x.minword)
			rand = True if hasattr(x, "randomTweet") else False
			displayTweet = int(x.displayTweets)
		except:
			traceback.print_exc()
			#print k,iteration,p,distance,seed,nbTweets,rand,minWord,displayTweet
			return render.index({},[],"One or more of parameters is/are invalid")

		clustering = Cluster()
		print "Random : ", rand
		docList = clustering.toDocList(clustering.retrieveTweets(nbTweets,minWord,rand))
		cluster = clustering.kmeansCluster(docList, k, iteration, distance, seed, p)
		#clusterStrList = str(clustering.sampleCluster(cluster, 2)).replace("u", "")
		clusterStrList = clustering.sampleCluster(cluster, displayTweet)

		params = {"k": k, "p": p, "iteration": iteration, "distance": distance, "seed": seed}
		return render.index(params, clusterStrList, "") 

if __name__ == "__main__":
	app = web.application(urls, globals())
	app.internalerror = web.debugerror
	app.run()


