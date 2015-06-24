from semantics import TwitterMiniBatch, LDAModel, ModelDict
from datastream import FeedListener
import http, time
from threading import Thread
from patterns import Singleton
import sys

@Singleton
class Main:
	def __init__(self):
		self.model = None

		self.words = []

		self.httpThread = Thread(target=self.starthttp)
		self.httpThread.setDaemon(True)

	def starthttp(self):
		http.run()
	
	def feedback(self, msg):
		word = msg['word'].lower().strip()

		if word in self.words:
			return;

		self.words.append(word)

		self.dict.add_word(word)
		print "'%s' added"%word
		return
	
	def run(self):
		
		if len(sys.argv) != 5:
			print "Usage: " + sys.argv[0] + " <port> <modelname> <dictionarySize> <redis-host>"
			exit(1)

		modelname = sys.argv[2]
		dictionarysize = int(sys.argv[3])
		redishost = sys.argv[4]
		self.httpThread.start()

		modelDict = ModelDict.Instance()
		modelDict.init(modelname, dictionarysize)

		self.model = modelDict.model
		self.dict = modelDict.dict

		#print self.model.show_topics(20)
		if self.model is not None:
			print "Topic model initialized"
		#print self.model.lda

		tmb = TwitterMiniBatch(100)
		tmb.setDictionary(self.dict)

		streamfeed = FeedListener(redishost)
		streamfeed.setStreamHandler(tmb)
		streamfeed.setFeedbackHandler(self)
		streamfeed.startListen()

		b = 1
		lasttime = time.time()
		while True:
			time.sleep(0.001)
			if not tmb.hasData():
				continue

			self.model.trainModel(tmb)

			enr = tmb.enrichDoc(self.model)
			
			streamfeed.publish(enr)

			tmb.queuePop()
			b += 1

			thistime = time.time()
			deltatime = thistime - lasttime
			rate = tmb.buffersize / deltatime

			print "Buffer size: %d, Tweet count : %d, Rate: %d tweets/s"%(tmb.buffersize, b*tmb.buffersize, rate)

			lasttime = thistime

	
if __name__ == "__main__":
	main = Main.Instance()
	main.run()
