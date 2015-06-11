from semantics import SuperDictionary, TwitterMiniBatch, LDAModel
from datastream import FeedListener
import http, time
from threading import Thread
from dictionary import WrapperDictionary


class Main:
	def __init__(self):
		self.model = None

		self.httpThread = Thread(target=self.starthttp)
		self.httpThread.setDaemon(True)
		self.httpThread.start()

	def starthttp(self):
		http.run()
	
	def run(self):
		#sdict = SuperDictionary()
		wdict = WrapperDictionary.Instance()
		wdict.init(1000)
		tmb = TwitterMiniBatch()

		tmb.setDictionary(wdict)

		self.model = LDAModel.Instance()
		self.model.setName("model1")
		self.model.setDictionary(wdict)
		self.model.initialize()

		#print self.model.show_topics(20)
		print self.model

		#self.model = LDAModel("model1", sdict.dictionary)

		streamfeed = FeedListener()
		streamfeed.setDataHandler(tmb)
		streamfeed.startListen()

		b = 1
		while True:
			time.sleep(0.001)
			if not tmb.hasData():
				continue

			print "=========Batch " + str(b) + "=========="
			self.model.trainModel(tmb)

			enr = tmb.enrichDoc(self.model)
			
			streamfeed.publish(enr)

			print str(len(enr)) + " enriched "

			tmb.queuePop()
			b += 1
			print "================== "

	
if __name__ == "__main__":
	main = Main()
	main.run()
